package social

import (
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
	ID        uint64     `json:"id"`
	BlogID    uint64     `json:"blogId"`
	AuthorID  uint64     `json:"authorId"`
	ParentID  *uint64    `json:"parentId,omitempty"`
	Content   string     `json:"content"`
	CreatedAt time.Time  `json:"createdAt" gorm:"->"`
	DeletedAt *time.Time `json:"deletedAt,omitempty"`
}

const commentTable = "social_blog_comments"

var errCommentOwner = errors.New("comment author required")

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
	item := comment{BlogID: blogID, AuthorID: viewer, ParentID: body.ParentID, Content: body.Content}
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var blog Blog
		if err := tx.Table("tb_blog").Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Take(&blog, "id = ?", blogID).Error; err != nil {
			return err
		}
		if item.ParentID != nil {
			var parent comment
			if err := tx.Table(commentTable).Take(&parent, "id = ? AND blog_id = ? AND parent_id IS NULL AND deleted_at IS NULL", *item.ParentID, blogID).Error; err != nil {
				return err
			}
		}
		if err := tx.Table(commentTable).Create(&item).Error; err != nil {
			return err
		}
		return tx.Exec("UPDATE tb_blog SET comments = COALESCE(comments, 0) + 1 WHERE id = ?", blogID).Error
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
	query := s.db.WithContext(ctx).Table(commentTable).Where("blog_id = ? AND id > ?", blogID, after)
	if parent := c.Query("parentId"); parent != "" {
		id, ok := positiveID(c, parent)
		if !ok {
			return
		}
		query = query.Where("parent_id = ? AND deleted_at IS NULL", id)
	} else {
		query = query.Where("parent_id IS NULL AND (deleted_at IS NULL OR EXISTS (SELECT 1 FROM social_blog_comments r WHERE r.parent_id = social_blog_comments.id AND r.blog_id = social_blog_comments.blog_id AND r.deleted_at IS NULL))")
	}
	items := make([]comment, 0, 21)
	if err := query.Order("id").Limit(21).Find(&items).Error; err != nil {
		s.commentError(c, err)
		return
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
		var blog Blog
		if err := tx.Table("tb_blog").Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Take(&blog, "id = ?", blogID).Error; err != nil {
			return err
		}
		var item comment
		if err := tx.Table(commentTable).Take(&item, "id = ? AND blog_id = ?", commentID, blogID).Error; err != nil {
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
		return tx.Exec("UPDATE tb_blog SET comments = GREATEST(CAST(COALESCE(comments, 0) AS SIGNED) - 1, 0) WHERE id = ?", blogID).Error
	})
	if err != nil {
		s.commentError(c, err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", nil)
}

func (s *Service) commentError(c *gin.Context, err error) {
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
