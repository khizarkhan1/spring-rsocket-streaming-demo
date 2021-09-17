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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Controller
public class RSocketController {

    static final String SERVER = "Server";
    static final String RESPONSE = "Response";
    static final String STREAM = "Stream";
    static final String CHANNEL = "Channel";

    private final List<RSocketRequester> CLIENTS = new ArrayList<>();
    private final Map<String, RSocketRequester> clientsMap = new HashMap<>();

    @PreDestroy
    void shutdown() {
        log.info("Detaching all remaining clients...");
        CLIENTS.stream().forEach(requester -> requester.rsocket().dispose());
        log.info("Shutting down.");
    }

    @ConnectMapping("shell-client")
    void connectShellClientAndAskForTelemetry(RSocketRequester requester,
                                              @Payload String client) {

        requester.rsocket()
                .onClose()
                .doFirst(() -> {
                    // Add all new clients to a client list
                    log.info("Client: {} CONNECTED.", client);
                    CLIENTS.add(requester);
                    clientsMap.put(client, requester);
                })
                .doOnError(error -> {
                    // Warn when channels are closed by clients
                    log.warn("Channel to client {} CLOSED", client);
                })
                .doFinally(consumer -> {
                    // Remove disconnected clients from the client list
                    CLIENTS.remove(requester);
                    clientsMap.remove(client);
                    log.info("Client {} DISCONNECTED", client);
                })
                .subscribe();

        // Callback to client, confirming connection
//        requester.route("client-status")
//                .data("OPEN")
//                .retrieveFlux(String.class)
//                .doOnNext(s -> log.info("Client: {} Free Memory: {}.", client, s))
//                .subscribe();
    }
    

    /**
     * This @MessageMapping is intended to be used "stream <--> stream" style.
     * The incoming stream contains the interval settings (in seconds) for the outgoing stream of messages.
     *
     * @param messageFlux
     * @return
     */
    
    @MessageMapping("channel")
    Flux<Message> channel(@Payload Flux<Message> messageFlux) {
        log.info("Received channel request...");
        
        
        return messageFlux
                    .doOnNext(message1 -> System.out.println("Sender : " + message1.getSender() +
                            " Receiver : " + message1.getReceiver() +
                            " Content : " + message1.getContent()))
                    .switchMap(message1 ->
                        clientsMap.get(message1.getReceiver())
                                .route("channel")
                                .data(Flux.just(message1))
                                .retrieveFlux(Message.class)
                    )
                    .log();
        
    }
}
