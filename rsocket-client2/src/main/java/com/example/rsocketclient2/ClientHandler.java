package com.example.rsocketclient2;

import com.example.rsocketclient2.data.Client;
import com.example.rsocketclient2.data.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import reactor.core.publisher.Flux;

import java.time.Duration;

@Slf4j
public class ClientHandler {
	
	@MessageMapping("client-status")
	public Flux<String> statusUpdate(String status) {
		log.info("Connection {}", status);
		return Flux
				.interval(Duration.ofSeconds(5))
				.map(index ->
					String.valueOf(
						Runtime.getRuntime().freeMemory()
					)
				)
		;
	}
	
	@MessageMapping("call")
	public Flux<Message> channel(@Payload Flux<Message> messageFlux){
		return messageFlux
				.doOnNext(message ->
					System.out.println(
						"Sender : " + message.getSender() +
						"\nReceiver : " + message.getReceiver() +
						"\nContent : " + message.getContent()
					)
				)
				.switchMap(message -> {
					message.setContent("Hello from " + Client.CLIENT_ID);
					message.setSender(Client.CLIENT_ID);
					message.setReceiver(message.getSender());
					return Flux.just(message);
				})
		;
	}
}
