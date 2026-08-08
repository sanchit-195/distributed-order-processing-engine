package com.example.order.controller;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public class OrderRequest {

    @NotNull(message = "productId is required")
    private Long productId;

    @NotNull(message = "quantity is required")
    @Min(value = 1, message = "quantity must be at least 1")
    private Integer quantity;

    // Phase 2's order-service no longer owns Product data (inventory-service
    // does), so it can't look up the price itself. The caller supplies it
    // here. In a more complete system this would instead be a synchronous
    // lookup to inventory-service before the saga starts.
    @NotNull(message = "unitPrice is required")
    @DecimalMin(value = "0.01", message = "unitPrice must be positive")
    private BigDecimal unitPrice;

    public OrderRequest() {
    }

    public OrderRequest(Long productId, Integer quantity, BigDecimal unitPrice) {
        this.productId = productId;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }

    public Long getProductId() {
        return productId;
    }

    public void setProductId(Long productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public void setUnitPrice(BigDecimal unitPrice) {
        this.unitPrice = unitPrice;
    }
}