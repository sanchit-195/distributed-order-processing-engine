package com.example.order.controller;

import com.example.order.entity.Order;
import com.example.order.service.OrderService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public ResponseEntity<Order> placeOrder(@Valid @RequestBody OrderRequest request) {
        // 202 Accepted, not 201 Created - unlike Phase 1, we don't yet
        // know if this order will succeed. It's accepted for processing;
        // the saga determines the final CONFIRMED/FAILED outcome
        // asynchronously. The caller has to poll GET /orders/{id} (or a
        // future webhook/notification) to find out what happened.
        Order order = orderService.placeOrder(
                request.getProductId(), request.getQuantity(), request.getUnitPrice());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(order);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getOrder(@PathVariable Long id) {
        return orderService.findById(id)
                .map(ResponseEntity::ok)
                .orElseThrow(() -> new ResponseStatusException(
                        HttpStatus.NOT_FOUND, "No order with id " + id));
    }
}