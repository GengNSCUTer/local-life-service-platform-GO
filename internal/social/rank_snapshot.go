package social

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const rankInterval = 10 * time.Second

func rankKey(version int64) string { return "social:rank:v1:" + strconv.FormatInt(version, 10) }

func (s *Service) rankWorker(ctx context.Context) {
	for ctx.Err() == nil {
		operation, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
		version := time.Now().Unix() / int64(rankInterval/time.Second)
		err := s.generateRank(operation, version)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("refresh social rank snapshot", "error", err)
		}
		if !wait(ctx, rankInterval) {
			return
		}
	}
}

func (s *Service) generateRank(ctx context.Context, version int64) error {
	key := rankKey(version)
	exists, err := s.cache.Exists(ctx, key).Result()
	if err != nil || exists != 0 {
		return err
	}
	var ids []uint64
	if err := s.db.WithContext(ctx).Table("tb_blog FORCE INDEX (idx_blog_hot)").Order("liked DESC, id DESC").Limit(1000).Pluck("id", &ids).Error; err != nil {
		return err
	}
	payload, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	return s.cache.SetNX(ctx, key, payload, time.Minute).Err()
}

func (s *Service) snapshotHot(c *gin.Context) {
	ctx, cancel, viewer := requestContext(c)
	defer cancel()
	value := c.DefaultQuery("cursor", "0")
	latest := time.Now().Unix() / int64(rankInterval/time.Second)
	version, offset := latest, 0
	if value != "0" {
		prefix, page, ok := strings.Cut(value, ":")
		var versionErr, pageErr error
		version, versionErr = strconv.ParseInt(prefix, 10, 64)
		offset, pageErr = strconv.Atoi(page)
		if !ok || versionErr != nil || pageErr != nil || offset <= 0 || offset >= 1000 || offset%pageSize != 0 || version <= 0 || version > latest {
			httpx.Respond(c, http.StatusBadRequest, "invalid rank cursor", nil)
			return
		}
	}
	operation, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop()
	var payload []byte
	var err error
	for attempts := 0; attempts < 3; attempts++ {
		payload, err = s.cache.Get(operation, rankKey(version)).Bytes()
		if !errors.Is(err, redis.Nil) || value != "0" {
			break
		}
		version--
	}
	if errors.Is(err, redis.Nil) && value != "0" {
		httpx.Respond(c, http.StatusGone, "rank snapshot expired; refresh the first page", nil)
		return
	}
	var ids []uint64
	if err != nil || json.Unmarshal(payload, &ids) != nil {
		httpx.Respond(c, http.StatusServiceUnavailable, "rank snapshot temporarily unavailable", nil)
		return
	}
	stop()
	result := hotPage{List: make([]blogView, 0, pageSize)}
	if offset < len(ids) {
		end := min(offset+pageSize, len(ids))
		var rows []blogView
		if err := s.blogs(ctx, viewer).Where("b.id IN ?", ids[offset:end]).Find(&rows).Error; err != nil {
			s.respond(c, nil, err)
			return
		}
		byID := make(map[uint64]blogView, len(rows))
		for _, row := range rows {
			byID[row.ID] = row
		}
		for _, id := range ids[offset:end] {
			if row, ok := byID[id]; ok {
				result.List = append(result.List, row)
			}
		}
		if end < len(ids) {
			result.NextCursor = strconv.FormatInt(version, 10) + ":" + strconv.Itoa(end)
		}
	}
	s.respond(c, result, nil)
}
