package shop

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/GengNSCUTer/local-life-service-platform-GO/internal/platform/httpx"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const geoLatitudeLimit = 85.05112878

func (s *Service) nearby(c *gin.Context, typeID uint64, longitude, latitude string) {
	x, xErr := strconv.ParseFloat(longitude, 64)
	y, yErr := strconv.ParseFloat(latitude, 64)
	page, pageErr := strconv.Atoi(c.DefaultQuery("current", "1"))
	if xErr != nil || yErr != nil || !validGeo(x, y) || pageErr != nil || page < 1 || page > 1000 {
		httpx.Respond(c, http.StatusBadRequest, "valid x, y and page (1–1000) are required", nil)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), requestTimeout)
	defer cancel()
	items, err := s.findNearby(ctx, typeID, x, y, page)
	if err != nil {
		s.respondError(c, "find nearby shops", err)
		return
	}
	httpx.Respond(c, http.StatusOK, "", items)
}

func validGeo(x, y float64) bool {
	return math.Abs(x) <= 180 && math.Abs(y) <= geoLatitudeLimit
}

type nearbyShop struct {
	shopRecord
	Distance float64 `json:"distance"`
}

func (s *Service) findNearby(ctx context.Context, kind uint64, x, y float64, page int) ([]nearbyShop, error) {
	items := make([]nearbyShop, 0)
	key, err := s.geoIndex(ctx, kind)
	if err != nil {
		return nil, err
	}
	const size = 5
	locations, err := s.cache.GeoSearchLocation(ctx, key, &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{Longitude: x, Latitude: y, Radius: 5000, RadiusUnit: "m", Sort: "ASC", Count: page * size},
		WithDist:       true,
	}).Result()
	if err != nil {
		return nil, err
	}
	start := (page - 1) * size
	if len(locations) <= start {
		return items, nil
	}
	locations = locations[start:]
	ids := make([]uint64, 0, len(locations))
	for _, location := range locations {
		id, err := strconv.ParseUint(location.Name, 10, 64)
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	var rows []shopRecord
	if err := s.shops.db.WithContext(ctx).Table(shopTable).Where("id IN ? AND type_id = ?", ids, kind).Find(&rows).Error; err != nil {
		return nil, err
	}
	byID := make(map[uint64]shopRecord, len(rows))
	for _, row := range rows {
		byID[row.ID] = row
	}
	for i, id := range ids {
		if row, ok := byID[id]; ok {
			items = append(items, nearbyShop{row, locations[i].Dist})
		}
	}
	return items, nil
}

func (s *Service) geoIndex(ctx context.Context, kind uint64) (string, error) {
	var revision uint64
	if err := s.shops.db.WithContext(ctx).Raw("SELECT COALESCE(MAX(revision), 0) FROM shop_geo_versions WHERE type_id = ?", kind).Scan(&revision).Error; err != nil {
		return "", err
	}
	key := fmt.Sprintf("shop:geo:{%d}:%d", kind, revision)
	ready := key + ":ready"
	present, err := s.cache.Exists(ctx, ready).Result()
	if err != nil || present > 0 {
		return key, err
	}
	var rows []struct {
		ID   uint64
		X, Y float64
	}
	if err := s.shops.db.WithContext(ctx).Table(shopTable).Select("id, x, y").Where("type_id = ?", kind).Find(&rows).Error; err != nil {
		return "", err
	}
	locations := make([]*redis.GeoLocation, 0, len(rows))
	for _, row := range rows {
		if validGeo(row.X, row.Y) {
			locations = append(locations, &redis.GeoLocation{Name: strconv.FormatUint(row.ID, 10), Longitude: row.X, Latitude: row.Y})
		}
	}
	_, err = s.cache.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, key)
		if len(locations) > 0 {
			pipe.GeoAdd(ctx, key, locations...)
			pipe.Expire(ctx, key, 5*time.Minute+time.Second)
		}
		pipe.Set(ctx, ready, "1", 5*time.Minute)
		return nil
	})
	return key, err
}

func bumpGeo(tx *gorm.DB, kind uint64) error {
	return tx.Exec(`INSERT INTO shop_geo_versions (type_id, revision) VALUES (?, 1)
		ON DUPLICATE KEY UPDATE revision = revision + 1`, kind).Error
}
