package com.firsthorizon.fraud.kafka.streaming.app.contollers;

import com.firsthorizon.fraud.kafka.streaming.app.models.Order;
import org.kie.api.runtime.KieSession;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@RestController
public class AppController {

	@Autowired
	private KieSession session;

	@PostMapping("/api/v1/order")
	public Order orderNow(@RequestBody Order order) {
		session.insert(order);
		session.fireAllRules();
		return order;
	}
	
	@GetMapping("/api/v1/verify")
	public String getOrder() {
		return "API Testing ..";
	}
	
	@GetMapping("/api/v1/view/order/")
	public Order getOrder(@RequestBody Order order) {
		session.insert(order);
		session.fireAllRules();
		return order;
	}

}