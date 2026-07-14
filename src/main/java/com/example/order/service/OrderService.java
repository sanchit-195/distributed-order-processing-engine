package com.example.order.service;

import com.example.order.entity.Order;
import com.example.order.entity.Payment;
import com.example.order.entity.Product;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.PaymentRepository;
import com.example.order.repository.ProductRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.Optional;

@Service
public class OrderService {

    private final ProductRepository productRepository;
    private final PaymentRepository paymentRepository;
    private final OrderRepository orderRepository;

    public OrderService(ProductRepository productRepository,
                         PaymentRepository paymentRepository,
                         OrderRepository orderRepository) {
        this.productRepository = productRepository;
        this.paymentRepository = paymentRepository;
        this.orderRepository = orderRepository;
    }

    /**
     * The whole point of Phase 1: everything below runs as ONE local ACID
     * transaction. If any step throws, Spring rolls back all of it —
     * the stock deduction, the payment row, and the order row all
     * disappear together. There is no possibility of "stock deducted but
     * no order created" the way there will be once this becomes 3
     * separate services in Phase 2.
     */
    @Transactional
    public Order placeOrder(Long productId, Integer quantity) {

        // 1. Lock the product row (SELECT ... FOR UPDATE) so no other
        //    concurrent transaction can read/modify its stock until we commit.
        Product product = productRepository.findByIdForUpdate(productId)
                .orElseThrow(() -> new ProductNotFoundException(
                        "No product with id " + productId));

        // 2. Check stock while holding the lock.
        if (product.getStock() < quantity) {
            throw new InsufficientStockException(
                    "Only " + product.getStock() + " units left for product " + productId);
        }

        // 3. Deduct stock. This UPDATE is part of the same transaction,
        //    still protected by the row lock from step 1.
        product.setStock(product.getStock() - quantity);
        productRepository.save(product);

        BigDecimal totalAmount = product.getPrice().multiply(BigDecimal.valueOf(quantity));

        // 4. Create the order in PENDING first, mirroring the state
        //    machine the saga will use in Phase 2 (PENDING -> CONFIRMED).
        //    Here it's just bookkeeping since everything commits atomically,
        //    but it keeps the mental model consistent with later phases.
        Order order = new Order(productId, quantity, totalAmount, Order.OrderStatus.PENDING);
        order = orderRepository.save(order);

        // 5. "Process payment" — in the monolith this is a synchronous,
        //    in-process call, not a network hop. No compensation logic is
        //    needed yet because everything is one transaction: if this
        //    were to fail, the whole method would throw and the stock
        //    deduction above would be rolled back automatically.
        Payment payment = new Payment(order.getId(), totalAmount, Payment.PaymentStatus.SUCCESS);
        paymentRepository.save(payment);

        // 6. Mark the order confirmed now that payment succeeded.
        order.setStatus(Order.OrderStatus.CONFIRMED);
        return orderRepository.save(order);
    }

    @Transactional(readOnly = true)
    public Optional<Order> findById(Long id) {
        return orderRepository.findById(id);
    }
}
