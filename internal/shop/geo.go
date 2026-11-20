package shop

import (
	"context"
	"crypto/rand"
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
	query := &redis.GeoSearchLocationQuery{
		GeoSearchQuery: redis.GeoSearchQuery{Longitude: x, Latitude: y, Radius: 5000, RadiusUnit: "m", Sort: "ASC", Count: page * size},
		WithDist:       true,
	}
	locations, err := s.cache.GeoSearchLocation(ctx, key, query).Result()
	if err != nil {
		return nil, err
	}
	if len(locations) == 0 {
		// The ZSET can be evicted independently of its marker after geoIndex returns.
		ready, err := geoReady.Run(ctx, s.cache, []string{key, key + ":ready"}).Int()
		if err != nil {
			return nil, err
		}
		if ready == 0 {
			key, err = s.geoIndex(ctx, kind)
			if err != nil {
				return nil, err
			}
			locations, err = s.cache.GeoSearchLocation(ctx, key, query).Result()
			if err != nil {
				return nil, err
			}
		}
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

// The index, readiness marker and lease share the category's Redis hash tag.
const checkGeoReady = `
local ready = redis.call('GET', KEYS[2])
local indexReady = ready == '0' or (ready == '1' and redis.call('EXISTS', KEYS[1]) == 1)
`

var geoReady = redis.NewScript(checkGeoReady + `return indexReady and 1 or 0`)

var claimGeoBuild = redis.NewScript(checkGeoReady + `
if indexReady then return 2 end
if redis.call('SET', KEYS[3], ARGV[1], 'NX', 'PX', ARGV[2]) then return 1 end
return 0
`)

var finishGeoBuild = redis.NewScript(`
if redis.call('GET', KEYS[3]) ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
-- Bound the size of each GEOADD argument list even for large categories.
for first = 4, #ARGV, 600 do
    redis.call('GEOADD', KEYS[1], unpack(ARGV, first, math.min(first + 599, #ARGV)))
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('SET', KEYS[2], #ARGV > 3 and '1' or '0', 'PX', ARGV[2])
redis.call('DEL', KEYS[3])
return 1
`)

var releaseGeoBuild = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`)

func (s *Service) geoIndex(ctx context.Context, kind uint64) (string, error) {
	var revision uint64
	if err := s.shops.db.WithContext(ctx).Raw("SELECT COALESCE(MAX(revision), 0) FROM shop_geo_versions WHERE type_id = ?", kind).Scan(&revision).Error; err != nil {
		return "", err
	}
	key := fmt.Sprintf("shop:geo:{%d}:%d", kind, revision)
	ready, lease := key+":ready", key+":build"
	present, err := geoReady.Run(ctx, s.cache, []string{key, ready}).Int()
	if err != nil || present > 0 {
		return key, err
	}
	token := rand.Text()
	for {
		state, err := claimGeoBuild.Run(ctx, s.cache, []string{key, ready, lease}, token, (2 * time.Second).Milliseconds()).Int()
		if err != nil || state == 2 {
			return key, err
		}
		if state == 1 {
			built, err := s.buildGeoIndex(ctx, kind, key, ready, lease, token)
			if err != nil || built {
				return key, err
			}
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (s *Service) buildGeoIndex(ctx context.Context, kind uint64, key, ready, lease, token string) (bool, error) {
	var rows []struct {
		ID   uint64
		X, Y float64
	}
	if err := s.shops.db.WithContext(ctx).Table(shopTable).Select("id, x, y").Where("type_id = ?", kind).Find(&rows).Error; err != nil {
		s.releaseGeoLease(ctx, lease, token)
		return false, err
	}
	const ttl = 5 * time.Minute
	args := make([]any, 0, 3+len(rows)*3)
	args = append(args, token, ttl.Milliseconds(), (ttl + time.Second).Milliseconds())
	for _, row := range rows {
		if validGeo(row.X, row.Y) {
			args = append(args, row.X, row.Y, strconv.FormatUint(row.ID, 10))
		}
	}
	built, err := finishGeoBuild.Run(ctx, s.cache, []string{key, ready, lease}, args...).Int()
	if err != nil {
		s.releaseGeoLease(ctx, lease, token)
	}
	return built == 1, err
}

func (s *Service) releaseGeoLease(ctx context.Context, lease, token string) {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 100*time.Millisecond)
	defer cancel()
	_ = releaseGeoBuild.Run(cleanup, s.cache, []string{lease}, token).Err()
}

func bumpGeo(tx *gorm.DB, kind uint64) error {
	return tx.Exec(`INSERT INTO shop_geo_versions (type_id, revision) VALUES (?, 1)
		ON DUPLICATE KEY UPDATE revision = revision + 1`, kind).Error
}
