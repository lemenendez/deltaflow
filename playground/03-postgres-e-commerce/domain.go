package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	hostpkg "github.com/lemenendez/deltaflow/internal/host"
	deltaflow "github.com/lemenendez/deltaflow/pkg/deltaflow"
)

type product struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Description     string         `json:"description"`
	ImageURL        string         `json:"image_url"`
	BasePriceCents  int            `json:"base_price_cents"`
	DiscountPct     int            `json:"discount_pct"`
	Inventory       map[string]int `json:"inventory"`
	FreeShipping    bool           `json:"free_shipping"`
	Promotion       string         `json:"promotion"`
	CheckoutWording string         `json:"checkout_wording"`
	Version         int            `json:"version"`
	UpdatedAt       string         `json:"updated_at"`
}

type productSearchDocument struct {
	ProductID       string         `json:"product_id"`
	Name            string         `json:"name"`
	Description     string         `json:"description"`
	ImageURL        string         `json:"image_url"`
	BasePriceCents  int            `json:"base_price_cents"`
	DiscountPct     int            `json:"discount_pct"`
	SalePriceCents  int            `json:"sale_price_cents"`
	Inventory       map[string]int `json:"inventory"`
	InventoryTotal  int            `json:"inventory_total"`
	Available       bool           `json:"available"`
	FreeShipping    bool           `json:"free_shipping"`
	Promotion       string         `json:"promotion"`
	CheckoutWording string         `json:"checkout_wording"`
	Version         int            `json:"version"`
	UpdatedAt       string         `json:"updated_at"`
}

type catalogStore struct {
	db *sql.DB
}

type searchTarget interface {
	deltaflow.ProjectionApplier
	snapshot(ctx context.Context) (map[string][]byte, int, int, int, error)
}

func newCatalogStore(db *sql.DB) *catalogStore {
	return &catalogStore{db: db}
}

func (s *catalogStore) ensureSchema(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
CREATE SCHEMA IF NOT EXISTS playground;

CREATE TABLE IF NOT EXISTS playground.ecommerce_products (
	product_id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	description TEXT NOT NULL,
	image_url TEXT NOT NULL,
	base_price_cents INTEGER NOT NULL,
	discount_pct INTEGER NOT NULL,
	inventory JSONB NOT NULL,
	free_shipping BOOLEAN NOT NULL,
	promotion TEXT NOT NULL,
	checkout_wording TEXT NOT NULL,
	version INTEGER NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
);`)
	return err
}

func (s *catalogStore) reset(ctx context.Context, products []product) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `DELETE FROM playground.ecommerce_products`); err != nil {
		return err
	}

	for _, p := range products {
		if err := s.insertProduct(ctx, tx, p); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *catalogStore) insertProduct(ctx context.Context, tx *sql.Tx, p product) error {
	inventory, err := json.Marshal(p.Inventory)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
INSERT INTO playground.ecommerce_products (
	product_id,
	name,
	description,
	image_url,
	base_price_cents,
	discount_pct,
	inventory,
	free_shipping,
	promotion,
	checkout_wording,
	version,
	updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $12)`,
		p.ID,
		p.Name,
		p.Description,
		p.ImageURL,
		p.BasePriceCents,
		p.DiscountPct,
		inventory,
		p.FreeShipping,
		p.Promotion,
		p.CheckoutWording,
		p.Version,
		parseTime(p.UpdatedAt),
	)
	return err
}

func (s *catalogStore) applyMutation(ctx context.Context, tx *sql.Tx, m mutation) error {
	setSQL := ""
	var value any
	switch m.Kind {
	case "description":
		setSQL = "description = $1"
		value = m.Value.(string)
	case "image":
		setSQL = "image_url = $1"
		value = m.Value.(string)
	case "discount":
		setSQL = "discount_pct = $1"
		value = m.Value.(int)
	case "inventory":
		setSQL = "inventory = $1::jsonb"
		raw, err := json.Marshal(m.Value.(map[string]int))
		if err != nil {
			return err
		}
		value = raw
	case "free_shipping":
		setSQL = "free_shipping = $1"
		value = m.Value.(bool)
	case "promotion":
		setSQL = "promotion = $1"
		value = m.Value.(string)
	case "checkout_wording":
		setSQL = "checkout_wording = $1"
		value = m.Value.(string)
	default:
		return fmt.Errorf("unsupported mutation kind %q", m.Kind)
	}

	res, err := tx.ExecContext(ctx, fmt.Sprintf(`
UPDATE playground.ecommerce_products
SET %s,
	version = version + 1,
	updated_at = $2
WHERE product_id = $3`, setSQL), value, m.At, m.ProductID)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("product %s not found", m.ProductID)
	}
	return nil
}

func (s *catalogStore) project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	if identity.Type != projectionType {
		return deltaflow.Projection{}, fmt.Errorf("unsupported projection type %q", identity.Type)
	}
	productID, err := hostpkg.StringFromKey(identity.Key, "product_id")
	if err != nil {
		return deltaflow.Projection{}, err
	}

	p, ok, err := s.getProduct(ctx, productID)
	if err != nil {
		return deltaflow.Projection{}, err
	}
	if !ok {
		return deltaflow.Projection{}, deltaflow.ErrProjectionNotFound
	}

	doc := toSearchDocument(p)
	return hostpkg.JSONProjection(identity, doc)
}

func (s *catalogStore) getProduct(ctx context.Context, productID string) (product, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT
	product_id,
	name,
	description,
	image_url,
	base_price_cents,
	discount_pct,
	inventory,
	free_shipping,
	promotion,
	checkout_wording,
	version,
	updated_at
FROM playground.ecommerce_products
WHERE product_id = $1`, productID)

	var p product
	var inventory []byte
	var updatedAt time.Time
	if err := row.Scan(
		&p.ID,
		&p.Name,
		&p.Description,
		&p.ImageURL,
		&p.BasePriceCents,
		&p.DiscountPct,
		&inventory,
		&p.FreeShipping,
		&p.Promotion,
		&p.CheckoutWording,
		&p.Version,
		&updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return product{}, false, nil
		}
		return product{}, false, err
	}
	if err := json.Unmarshal(inventory, &p.Inventory); err != nil {
		return product{}, false, err
	}
	p.UpdatedAt = updatedAt.UTC().Format(time.RFC3339)
	return p, true, nil
}

type searchIndexSimulator struct {
	mu          sync.Mutex
	docs        map[string][]byte
	failOnce    map[string]bool
	deadLetters map[string]bool
	upserts     int
	deletes     int
	failures    int
}

func newSearchIndexSimulator(failOnce map[string]bool, deadLetters map[string]bool) *searchIndexSimulator {
	return &searchIndexSimulator{
		docs:        map[string][]byte{"sku-ghost-001": []byte(`{"product_id":"sku-ghost-001","stale":true}`)},
		failOnce:    failOnce,
		deadLetters: deadLetters,
	}
}

func (i *searchIndexSimulator) apply(_ context.Context, op deltaflow.ProjectionOperation) error {
	productID, err := hostpkg.StringFromKey(op.Identity.Key, "product_id")
	if err != nil {
		return err
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	switch op.Type {
	case deltaflow.ProjectionOpUpsert:
		if op.Projection == nil {
			return errors.New("upsert operation requires projection")
		}
		if i.deadLetters[productID] {
			i.failures++
			return fmt.Errorf("elasticsearch rejected product %s: invalid image payload", productID)
		}
		if i.failOnce[productID] {
			delete(i.failOnce, productID)
			i.failures++
			return fmt.Errorf("elasticsearch temporary 429 for product %s", productID)
		}
		i.docs[productID] = append([]byte(nil), op.Projection.Payload...)
		i.upserts++
		return nil
	case deltaflow.ProjectionOpDelete:
		delete(i.docs, productID)
		i.deletes++
		return nil
	default:
		return fmt.Errorf("unsupported operation %q", op.Type)
	}
}

func (i *searchIndexSimulator) Apply(ctx context.Context, op deltaflow.ProjectionOperation) error {
	return i.apply(ctx, op)
}

func (i *searchIndexSimulator) snapshot(_ context.Context) (map[string][]byte, int, int, int, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	docs := make(map[string][]byte, len(i.docs))
	for key, value := range i.docs {
		docs[key] = append([]byte(nil), value...)
	}
	return docs, i.upserts, i.deletes, i.failures, nil
}

type countingProjector struct {
	projectFn func(context.Context, deltaflow.ProjectionIdentity) (deltaflow.Projection, error)
	ghosts    int64
}

func (p *countingProjector) Project(ctx context.Context, identity deltaflow.ProjectionIdentity) (deltaflow.Projection, error) {
	projection, err := p.projectFn(ctx, identity)
	if errors.Is(err, deltaflow.ErrProjectionNotFound) {
		atomic.AddInt64(&p.ghosts, 1)
	}
	return projection, err
}

func (p *countingProjector) ghostCount() int64 {
	return atomic.LoadInt64(&p.ghosts)
}

func toSearchDocument(p product) productSearchDocument {
	total := 0
	inventory := copyInventory(p.Inventory)
	for _, count := range inventory {
		total += count
	}
	salePrice := p.BasePriceCents * (100 - p.DiscountPct) / 100
	return productSearchDocument{
		ProductID:       p.ID,
		Name:            p.Name,
		Description:     p.Description,
		ImageURL:        p.ImageURL,
		BasePriceCents:  p.BasePriceCents,
		DiscountPct:     p.DiscountPct,
		SalePriceCents:  salePrice,
		Inventory:       inventory,
		InventoryTotal:  total,
		Available:       total > 0,
		FreeShipping:    p.FreeShipping,
		Promotion:       p.Promotion,
		CheckoutWording: p.CheckoutWording,
		Version:         p.Version,
		UpdatedAt:       p.UpdatedAt,
	}
}

func mutationValue(faker *gofakeit.Faker, kind string, seq int) any {
	switch kind {
	case "description":
		return faker.ProductDescription()
	case "image":
		return fmt.Sprintf("https://cdn.example.test/products/generated/%03d-%03d.jpg", seq, faker.Number(100, 999))
	case "discount":
		return faker.Number(0, 45)
	case "inventory":
		return randomInventory(faker)
	case "free_shipping":
		return faker.Number(0, 1) == 1
	case "promotion":
		return promotionCode(faker, seq)
	case "checkout_wording":
		return faker.Sentence(9)
	default:
		panic("unsupported mutation kind")
	}
}

func randomInventory(faker *gofakeit.Faker) map[string]int {
	return map[string]int{
		"atl": faker.Number(0, 120),
		"den": faker.Number(0, 120),
		"sea": faker.Number(0, 120),
	}
}

func copyInventory(in map[string]int) map[string]int {
	out := make(map[string]int, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func promotionCode(faker *gofakeit.Faker, seq int) string {
	raw := strings.ToUpper(faker.LetterN(5))
	return fmt.Sprintf("%s%02d", raw, seq%100)
}

func sortedProductIDs(products []product) []string {
	ids := make([]string, 0, len(products))
	for _, product := range products {
		ids = append(ids, product.ID)
	}
	sort.Strings(ids)
	return ids
}

func parseTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		panic(err)
	}
	return t
}

func printTopInventoryDocs(docs map[string][]byte, limit int) {
	type row struct {
		id    string
		total int
	}
	rows := make([]row, 0, len(docs))
	for id, payload := range docs {
		var doc productSearchDocument
		if err := json.Unmarshal(payload, &doc); err != nil {
			continue
		}
		rows = append(rows, row{id: id, total: doc.InventoryTotal})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].total == rows[j].total {
			return rows[i].id < rows[j].id
		}
		return rows[i].total > rows[j].total
	})
	if len(rows) > limit {
		rows = rows[:limit]
	}
	parts := make([]string, 0, len(rows))
	for _, row := range rows {
		parts = append(parts, fmt.Sprintf("%s:%d", row.id, row.total))
	}
	fmt.Printf("- Top indexed inventory totals: %s.\n", strings.Join(parts, ", "))
}
