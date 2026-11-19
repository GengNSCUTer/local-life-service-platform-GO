package seckill

import "testing"

func TestClassifyInventory(t *testing.T) {
	for _, tc := range []struct {
		name                                                                   string
		mysqlRemaining, confirmed, claims, resolutions, redisRemaining, buyers int64
		status, detail                                                         string
	}{
		{"pending reservations can outnumber confirmations", 8, 2, 2, 0, 3, 7, auditConsistent, "stock_counts_match"},
		{"durable cancellation can be retried", 8, 2, 2, 1, 3, 7, auditRepairable, "cancellation_resolutions_pending"},
		{"Redis stock modified independently", 8, 2, 2, 1, 4, 7, auditManual, "redis_stock_buyer_mismatch"},
		{"Redis stock and buyers lost together", 8, 2, 2, 0, 9, 1, auditManual, "redis_confirmed_buyers_missing"},
		{"MySQL stock modified independently", 8, 3, 3, 0, 3, 7, auditManual, "mysql_stock_mismatch"},
		{"MySQL claim missing", 8, 2, 1, 0, 3, 7, auditManual, "mysql_claim_count_mismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			activity := InventoryAudit{TotalStock: 10, MySQLRemaining: tc.mysqlRemaining,
				ConfirmedOrders: tc.confirmed, MySQLClaims: tc.claims, PendingResolutions: tc.resolutions}
			cache := redisInventory{stock: tc.redisRemaining, buyers: tc.buyers, startsAt: 100, endsAt: 200, present: true, valid: true}
			status, detail := classifyInventory(activity, cache, 100, 200)
			if status != tc.status || detail != tc.detail {
				t.Fatalf("got (%q, %q), want (%q, %q)", status, detail, tc.status, tc.detail)
			}
		})
	}
}
