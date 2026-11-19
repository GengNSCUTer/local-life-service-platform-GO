package user

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

const (
	codeTTL      = 2 * time.Minute
	codeCooldown = 30 * time.Second
	codeAttempts = 5
	sessionTTL   = 30 * time.Minute
	sessionRenew = 15 * time.Minute
)

var (
	errCodeDeliveryDisabled = errors.New("verification code delivery is disabled")
	errCodeCooldown         = errors.New("wait before requesting another verification code")
	errInvalidCode          = errors.New("invalid or expired verification code")
)

type Service struct {
	accounts    repository
	cache       *redis.Client
	logger      *slog.Logger
	devLogCodes bool
}

func New(db *gorm.DB, cache *redis.Client, logger *slog.Logger, devLogCodes bool) *Service {
	// The application logs operation failures without recording SQL containing phone numbers.
	db = db.Session(&gorm.Session{Logger: gormlogger.Default.LogMode(gormlogger.Silent)})
	return &Service{accounts: repository{db: db}, cache: cache, logger: logger, devLogCodes: devLogCodes}
}

func (s *Service) sendCode(ctx context.Context, phone string) error {
	if !s.devLogCodes {
		return errCodeDeliveryDisabled
	}
	number, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return fmt.Errorf("generate code: %w", err)
	}
	code := fmt.Sprintf("%06d", number.Int64())
	key := codeKey(phone)
	issued, err := issueCode.Run(ctx, s.cache, []string{key, key + ":cooldown"},
		code, codeTTL.Milliseconds(), codeCooldown.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("store code: %w", err)
	}
	if issued == 0 {
		return errCodeCooldown
	}
	s.logger.Info("development login code", "phone_suffix", phone[len(phone)-4:], "code", code)
	return nil
}

func (s *Service) login(ctx context.Context, phone, code string) (string, error) {
	matched, err := consumeCode.Run(ctx, s.cache, []string{codeKey(phone)}, code, codeAttempts).Int()
	if err != nil {
		return "", fmt.Errorf("verify code: %w", err)
	}
	if matched == 0 {
		return "", errInvalidCode
	}
	profile, err := s.accounts.findOrCreate(ctx, phone)
	if err != nil {
		return "", fmt.Errorf("find or create user: %w", err)
	}
	token := rand.Text()
	key := sessionKey(token)
	_, err = s.cache.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key, "id", profile.ID, "nickName", profile.NickName, "icon", profile.Icon, "renew", "1")
		pipe.Expire(ctx, key, sessionTTL)
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("save session: %w", err)
	}
	return token, nil
}
