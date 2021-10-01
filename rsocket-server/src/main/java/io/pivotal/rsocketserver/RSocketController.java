package io.pivotal.rsocketserver;

import io.pivotal.rsocketserver.data.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.annotation.ConnectMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Controller
public class RSocketController {
	
	//CuncurrentHashMap for storing clients with tread safety.
	private final Map<String, RSocketRequester> clientsMap = new ConcurrentHashMap<>();
	
	/**
	 * The annotation is called with the shutdown of server disposing all the connected
	 * clients.
	 */
	@PreDestroy
	void shutdown() {
		log.info("Detaching all remaining clients...");
		
		//Extract all the keys from the map.
		Set<String> keys = clientsMap.keySet();
		
		//For every key retrieve the requester and call dispose method.
		keys.forEach(requester -> {
			System.out.println("\n<<<Value " + clientsMap.get(requester) + ">>>\n");
			clientsMap.get(requester).rsocketClient().dispose();
			System.out.println("\n<<< isDisposed : " + clientsMap.get(requester).isDisposed() + ">>>\n");
		});
		log.info("Shutting down.");
	}
	
	/**
	 * The connectMapping is the end point which listen for all the incoming connection
	 * request takes RSocketRequester and String client as a parameter and store it in
	 * clientMap using client as a key and requester as a value.
	 *
	 * @param client	the name use for storing client
	 * @param requester the connection request from the client
	 */
	@ConnectMapping("connect")
	void connectClientAndAskForTelemetry(RSocketRequester requester, @Payload String client) {
		Objects.requireNonNull(requester.rsocket())
				.onClose()
				.doFirst(() -> {
					// Add all new clients to a client map
					log.info("Client: {} CONNECTED.", client);
					clientsMap.put(client, requester);
				})
				.doOnError(error -> {
					// Warn when channels are closed by clients
					log.warn("Channel to client {} CLOSED", client);
				})
				.doFinally(consumer -> {
					// Remove disconnected clients from the client map
					clientsMap.remove(client, requester);
					log.info("Client {} DISCONNECTED", client);
				})
				.doOnCancel(() ->{
					requester.dispose();
					clientsMap.remove(client, requester);
					System.out.println("Client Disconnected");
				})
				.subscribe(something -> System.out.println("Client disconnected : " + something));
		
		
		// Callback to client, confirming connection
		// And requesting telemetry data to keep connection aliv
		requester.route("client-status")
				.data("OPEN")
				.retrieveFlux(String.class)
				.doOnNext(s -> log.info("Client: {} Free Memory: {}.", client, s))
				.subscribe()
		;
	}
	
	
	/**
	 * This @MessageMapping is intended to be used "stream <--> stream" style.
	 * The incoming stream contains the voice data or video data as bytebuffer.
	 *
	 * @param messageFlux message to be sent to client
	 * @return return a new messageflux return by the client
	 */
	
	@MessageMapping("personal.call")
	Flux<Message> personalCall(@Payload Flux<Message> messageFlux) {
		log.info("Received channel request...");
		
		return messageFlux
				//For testing purpose to confirm content of message.
				//doOnNext takes the current flux and do some lambda expression on it.
				.doOnNext(currentMessage ->
						System.out.println(
								"Sender : " + currentMessage.getSender() +
										"\nReceiver : " + currentMessage.getReceiver() +
										"\nContent : " + currentMessage.getContent()
						)
				)
				//switchMap takes current flux as a parameter and return a new flux.
				.switchMap(message ->
						{
							//Using the current messageflux to extract the reciever and use it
							//to retrieve the requester assigned using this name and call this
							//requester method to target the client call end point.
							if (clientsMap.containsKey(message.getReceiver())){
								System.out.println("<<<not null>>>\n");
								return clientsMap
										//get the requester.
										.get(message.getReceiver())
										//target the call end point.
										.route("call")
										//send the data to end point.
										.data(Flux.just(message))
										//listen for the returned data.
										.retrieveFlux(Message.class);
							}
							//if the requester dose not exist return null.
							return null;
						}
				)
				//print logs to keep track of things.
				.log();
	}
	
	@MessageMapping("group.call")
	Flux<Message> groupCall(@Payload Flux<Message> messageFlux) {
		log.info("Received channel request...");
		
		return messageFlux
				.doOnNext(message1 ->
						System.out.println(
								"Sender : " + message1.getSender() +
										"\nReceiver : " + message1.getReceiver() +
										"\nContent : " + message1.getContent()
						)
				)
				.switchMap(message1 ->
						clientsMap
								.get(message1.getReceiver())
								.route("call")
								.data(Flux.just(message1))
								.retrieveFlux(Message.class)
				)
				.log();
	}
}
