package main

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

type TemplateServices struct {
	db  *pgxpool.Pool
	log *zap.Logger
}

type CreateTemplateRequest struct {
	Name     string `json:"name"     binding:"required"`
	Subject  string `json:"subject"  binding:"required"`
	BodyHTML string `json:"body_html" binding:"required"`
	BodyText string `json:"body_text" binding:"required"`
}

type Template struct {
	ID       uuid.UUID `json:"id"`
	TenantID uuid.UUID `json:"tenant_id"`
	Name     string    `json:"name"`
	Version  int       `json:"version"`
	Subject  string    `json:"subject"`
	BodyHTML string    `json:"body_html"`
	BodyText string    `json:"body_text"`
}

func (s *TemplateServices) Create(ctx *gin.Context) {
	tenantID, err := uuid.Parse(ctx.GetHeader("X-Tenant-ID"))
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid X-Tenant-ID"})
		return
	}

	var req CreateTemplateRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	id := uuid.New()

	_, err = s.db.Exec(ctx.Request.Context(), `
        INSERT INTO templates (id, tenant_id, name, subject, body_html, body_text, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
    `, id, tenantID, req.Name, req.Subject, req.BodyHTML, req.BodyText, time.Now())
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "failed to save template"})
		return
	}
	ctx.JSON(http.StatusCreated, gin.H{"id": id, "name": req.Name})

}

func (s *TemplateServices) Get(ctx *gin.Context) {
	var t Template
	err := s.db.QueryRow(ctx.Request.Context(), `
        SELECT id, tenant_id, name, version, subject, body_html, body_text
        FROM templates WHERE id = $1
    `, ctx.Param("id")).Scan(
		&t.ID, &t.TenantID, &t.Name, &t.Version,
		&t.Subject, &t.BodyHTML, &t.BodyText,
	)
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "template not found"})
		return
	}
	ctx.JSON(http.StatusOK, t)
}

func main() {

	log, _ := zap.NewProduction()
	defer log.Sync()

	db, _ := pgxpool.New(context.Background(), os.Getenv("DATABASE_URL"))

	svc := &TemplateServices{db: db, log: log}

	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/templates", svc.Create)
	r.GET("/templates/:id", svc.Get)
	r.GET("/healthz", func(ctx *gin.Context) { ctx.Status(http.StatusOK) })

	log.Info("template service listening", zap.String("addr", ":8082"))
	r.Run(":8082")

}
