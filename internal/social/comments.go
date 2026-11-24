package social

import (
	"crypto/sha256"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type comment struct {
	ID               uint64     `json:"id"`
	BlogID           uint64     `json:"blogId"`
	AuthorID         uint64     `json:"authorId"`
	ParentID         *uint64    `json:"parentId,omitempty"`
	ReplyToCommentID *uint64    `json:"replyToCommentId,omitempty"`
	RequestKey       []byte     `json:"-"`
	RequestDigest    []byte     `json:"-"`
	Content          string     `json:"content"`
	CreatedAt        time.Time  `json:"createdAt" gorm:"->"`
	DeletedAt        *time.Time `json:"deletedAt,omitempty"`
}

const commentTable = "social_blog_comments"

var errCommentOwner = errors.New("comment author required")
var errCommentRequestConflict = errors.New("idempotency key was already used for a different comment")

func (s *Service) createComment(c *gin.Context) {
	blogID, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	var body struct {
		ParentID *uint64 `json:"parentId"`
		Content  string  `json:"content"`
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 16<<10)
	if err := c.ShouldBindJSON(&body); err != nil {
		httpx.Respond(c, http.StatusBadRequest, "invalid comment", nil)
		return
	}
	body.Content = strings.TrimSpace(body.Content)
	if body.Content == "" || utf8.RuneCountInString(body.Content) > 1000 {
		httpx.Respond(c, http.StatusBadRequest, "comment must contain 1 to 1000 characters", nil)
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	item := comment{BlogID: blogID, AuthorID: viewer, Content: body.Content}
	if key := c.GetHeader("Idempotency-Key"); key != "" {
		if len(key) > 128 {
			httpx.Respond(c, http.StatusBadRequest, "invalid idempotency key", nil)
			return
		}
		keyDigest := sha256.Sum256([]byte(key))
		var target string
		if body.ParentID != nil {
			target = strconv.FormatUint(*body.ParentID, 10)
		}
		bodyDigest := sha256.Sum256([]byte(target + "\x00" + item.Content))
		item.RequestKey, item.RequestDigest = keyDigest[:], bodyDigest[:]
	}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.lockBlog(tx, blogID); err != nil {
			return err
		}
		if s.bucketCounters {
			if err := lockCounter(tx, blogID, viewer); err != nil {
				return err
			}
		}
		if len(item.RequestKey) != 0 {
			var existing comment
			err := tx.Table(commentTable).Select("id, request_digest").Take(&existing, "blog_id = ? AND author_id = ? AND request_key = ?", blogID, viewer, item.RequestKey).Error
			if err == nil {
				if string(existing.RequestDigest) != string(item.RequestDigest) {
					return errCommentRequestConflict
				}
				item.ID = existing.ID
				return nil
			}
			if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
		}
		if body.ParentID != nil {
			var parent comment
			if err := tx.Table(commentTable).Clauses(clause.Locking{Strength: "SHARE"}).Select("id, parent_id").Take(&parent, "id = ? AND blog_id = ? AND deleted_at IS NULL", *body.ParentID, blogID).Error; err != nil {
				return err
			}
			item.ParentID = &parent.ID
			if parent.ParentID != nil {
				item.ParentID = parent.ParentID
			}
			item.ReplyToCommentID = body.ParentID
		}
		if err := tx.Table(commentTable).Create(&item).Error; err != nil {
			return err
		}
		return s.changeCounters(tx, blogID, viewer, 0, 1)
	})
	if err != nil {
		s.commentError(c, err)
		return
	}
	httpx.Respond(c, http.StatusCreated, "", gin.H{"id": item.ID})
}

func (s *Service) listComments(c *gin.Context) {
	blogID, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	after, err := strconv.ParseUint(c.DefaultQuery("afterId", "0"), 10, 64)
	if err != nil {
		httpx.Respond(c, http.StatusBadRequest, "invalid afterId", nil)
		return
	}
	ctx, cancel, _ := requestContext(c)
	defer cancel()
	var blog Blog
	if err := s.db.WithContext(ctx).Table("tb_blog").Select("id").Take(&blog, "id = ?", blogID).Error; err != nil {
		s.commentError(c, err)
		return
	}
	query := s.db.WithContext(ctx).Table(commentTable+" FORCE INDEX (idx_comment_page)").
		Select("id, blog_id, author_id, parent_id, reply_to_comment_id, content, created_at, deleted_at").Where("blog_id = ? AND id > ?", blogID, after)
	if parent := c.Query("parentId"); parent != "" {
		id, ok := positiveID(c, parent)
		if !ok {
			return
		}
		query = query.Where("parent_id = ? AND deleted_at IS NULL", id)
	} else {
		query = query.Where("parent_id IS NULL AND (deleted_at IS NULL OR (SELECT r.id FROM social_blog_comments r FORCE INDEX (idx_comment_visible_reply) WHERE r.parent_id = social_blog_comments.id AND r.blog_id = social_blog_comments.blog_id AND r.deleted_at IS NULL LIMIT 1) IS NOT NULL)")
	}
	items := make([]comment, 0, 21)
	if err := query.Order("id").Limit(21).Find(&items).Error; err != nil {
		s.commentError(c, err)
		return
	}
	for i := range items {
		if items[i].ReplyToCommentID == nil {
			items[i].ReplyToCommentID = items[i].ParentID
		}
	}
	result := gin.H{}
	if len(items) > 20 {
		items = items[:20]
		result["nextAfterId"] = strconv.FormatUint(items[19].ID, 10)
	}
	result["list"] = items
	httpx.Respond(c, http.StatusOK, "", result)
}

func (s *Service) deleteComment(c *gin.Context) {
	blogID, ok := positiveID(c, c.Param("id"))
	if !ok {
		return
	}
	commentID, ok := positiveID(c, c.Param("commentId"))
	if !ok {
		return
	}
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := s.lockBlog(tx, blogID); err != nil {
			return err
		}
		if s.bucketCounters {
			if err := lockCounter(tx, blogID, viewer); err != nil {
				return err
			}
		}
		var item comment
		if err := tx.Table(commentTable).Clauses(clause.Locking{Strength: "UPDATE"}).Take(&item, "id = ? AND blog_id = ?", commentID, blogID).Error; err != nil {
			return err
		}
		if item.AuthorID != viewer {
			return errCommentOwner
		}
		if item.DeletedAt != nil {
			return nil
		}
		if err := tx.Table(commentTable).Where("id = ?", commentID).Updates(map[string]any{"deleted_at": time.Now().UTC(), "content": ""}).Error; err != nil {
			return err
		}
		return s.changeCounters(tx, blogID, viewer, 0, -1)
	})
	if err != nil {
		s.commentError(c, err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", nil)
}

func (s *Service) commentError(c *gin.Context, err error) {
	if errors.Is(err, errCommentRequestConflict) {
		httpx.Respond(c, http.StatusConflict, err.Error(), nil)
		return
	}
	if errors.Is(err, errCommentOwner) {
		httpx.Respond(c, http.StatusForbidden, err.Error(), nil)
		return
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		httpx.Respond(c, http.StatusNotFound, "blog or comment not found", nil)
		return
	}
	s.respond(c, nil, err)
}
