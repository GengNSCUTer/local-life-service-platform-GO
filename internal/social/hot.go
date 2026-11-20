package social

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
)

type hotPage struct {
	List       []blogView `json:"list"`
	NextCursor string     `json:"nextCursor,omitempty"`
}

func (s *Service) hot(c *gin.Context) {
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	cursor, cursorMode := c.GetQuery("cursor")
	// Scan just the ordered covering index and fetch full records for the selected page.
	page := s.db.WithContext(ctx).Table("tb_blog FORCE INDEX (idx_blog_hot)").
		Select("id, liked").Order("liked DESC, id DESC")
	if cursorMode {
		if _, hasCurrent := c.GetQuery("current"); hasCurrent {
			httpx.Respond(c, http.StatusBadRequest, "use cursor or current", nil)
			return
		}
		if cursor != "0" {
			score, row, found := strings.Cut(cursor, ":")
			liked, scoreErr := strconv.ParseUint(score, 10, 32)
			id, idErr := strconv.ParseUint(row, 10, 64)
			if !found || (scoreErr != nil && score != "null") || idErr != nil || id == 0 {
				httpx.Respond(c, http.StatusBadRequest, "invalid hot blog cursor", nil)
				return
			}
			// The Java schema allowed NULL likes and ordered them after zero.
			if score == "null" {
				page = page.Where("liked IS NULL AND id < ?", id)
			} else {
				page = page.Where("liked < ? OR (liked = ? AND id < ?) OR liked IS NULL", liked, liked, id)
			}
		}
		page = page.Limit(pageSize + 1)
	} else {
		current, err := strconv.Atoi(c.DefaultQuery("current", "1"))
		if err != nil || current < 1 || current > 10000 {
			httpx.Respond(c, http.StatusBadRequest, "page must be between 1 and 10000", nil)
			return
		}
		page = page.Offset((current - 1) * pageSize).Limit(pageSize)
	}
	items := make([]blogView, 0)
	err := s.blogs(ctx, viewer).Select(blogViewColumns+", ranked.liked AS rank_liked").
		Joins("JOIN (?) ranked ON ranked.id = b.id", page).
		Order("ranked.liked DESC, ranked.id DESC").Find(&items).Error
	if !cursorMode {
		s.respond(c, items, err)
		return
	}
	result := hotPage{List: items}
	if len(items) > pageSize {
		result.List = items[:pageSize]
		last := result.List[pageSize-1]
		score := "null"
		if last.RankLiked != nil {
			score = strconv.FormatUint(uint64(*last.RankLiked), 10)
		}
		result.NextCursor = score + ":" + strconv.FormatUint(last.ID, 10)
	}
	s.respond(c, result, err)
}
