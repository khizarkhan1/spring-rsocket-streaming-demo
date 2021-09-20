package com.example.rsocketclient2;


import com.example.rsocketclient2.data.Message;
import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.UUID;

@Slf4j
@ShellComponent
public class RSocketShellClient {

    private static final String CLIENT = "Client";
    private static final String REQUEST = "Request";
    private static final String FIRE_AND_FORGET = "Fire-And-Forget";
    private static final String STREAM = "Stream";
    private static final String CLIENT_ID = "client2";
    private static final MimeType SIMPLE_AUTH = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
    private static Disposable disposable;

    private RSocketRequester rsocketRequester;
    private RSocketRequester.Builder rsocketRequesterBuilder;
    private RSocketStrategies rsocketStrategies;

    @Autowired
    public RSocketShellClient(RSocketRequester.Builder builder,
                              @Qualifier("rSocketStrategies") RSocketStrategies strategies) {
        this.rsocketRequesterBuilder = builder;
        this.rsocketStrategies = strategies;
    }

    @ShellMethod("Login with your username and password.")
    public void login(String username, String password) {
        log.info("Connecting using client ID: {} and username: {}", CLIENT_ID, username);
        SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new ClientHandler());
        this.rsocketRequester = rsocketRequesterBuilder
                .setupRoute("shell-client")
                .setupData(CLIENT_ID)
                .rsocketConnector(connector -> connector.acceptor(responder))
                .connectTcp("localhost", 7000)
                .block();

        this.rsocketRequester.rsocket()
                .onClose()
                .doOnError(error -> log.warn("Connection CLOSED"))
                .doFinally(consumer -> log.info("Client DISCONNECTED"))
                .subscribe();
    }

    @PreDestroy
    @ShellMethod("Logout and close your connection")
    public void logout() {
        if (userIsLoggedIn()) {
            this.s();
            this.rsocketRequester.rsocket().dispose();
            log.info("Logged out.");
        }
    }

    private boolean userIsLoggedIn() {
        if (null == this.rsocketRequester || this.rsocketRequester.rsocket().isDisposed()) {
            log.info("No connection. Did you login?");
            return false;
        }
        return true;
    }

//    @ShellMethod("Send one request. One response will be printed.")
//    public void requestResponse() throws InterruptedException {
//        if (userIsLoggedIn()) {
//            log.info("\nSending one request. Waiting for one response...");
//            Message message = this.rsocketRequester
//                    .route("request-response")
//                    .data(new Message(CLIENT, REQUEST))
//                    .retrieveMono(Message.class)
//                    .block();
//            log.info("\nResponse was: {}", message);
//        }
//    }

//    @ShellMethod("Send one request. No response will be returned.")
//    public void fireAndForget() throws InterruptedException {
//        if (userIsLoggedIn()) {
//            log.info("\nFire-And-Forget. Sending one request. Expect no response (check server console log)...");
//            this.rsocketRequester
//                    .route("fire-and-forget")
//                    .data(new Message(CLIENT, FIRE_AND_FORGET))
//                    .send()
//                    .block();
//        }
//    }

//    @ShellMethod("Send one request. Many responses (stream) will be printed.")
//    public void stream() {
//        if (userIsLoggedIn()) {
//            log.info("\n\n**** Request-Stream\n**** Send one request.\n**** Log responses.\n**** Type 's' to stop.");
//            disposable = this.rsocketRequester
//                    .route("stream")
//                    .data(new Message(CLIENT, STREAM))
//                    .retrieveFlux(Message.class)
//                    .subscribe(message -> log.info("Response: {} \n(Type 's' to stop.)", message));
//        }
//    }

    @ShellMethod("Stream some settings to the server. Stream of responses will be printed.")
    public void channel() {
        if (userIsLoggedIn()) {
            log.info("\n\n***** Channel (bi-directional streams)\n***** Asking for a stream of messages.\n***** Type 's' to stop.\n\n");
            
            Message message = new Message(CLIENT_ID, "client1", "Hello From " + CLIENT_ID);
    
            this.rsocketRequester
                    .route("channel")
                    .data(Flux.just(message))
                    .retrieveFlux(Message.class)
                    .doOnNext(newMessage -> System.out.println("Sender : " + newMessage.getSender() +
                            " Receiver : " + newMessage.getReceiver() +
                            " Content : " + newMessage.getContent()))
                    .subscribe();
        }
    }

    @ShellMethod("Stops Streams or Channels.")
    public void s() {
        if (userIsLoggedIn() && null != disposable) {
            log.info("Stopping the current stream.");
            disposable.dispose();
            log.info("Stream stopped.");
        }
    }
}

@Slf4j
class ClientHandler {

    @MessageMapping("client-status")
    public Flux<String> statusUpdate(String status) {
        log.info("Connection {}", status);
        return Flux.interval(Duration.ofSeconds(5)).map(index -> String.valueOf(Runtime.getRuntime().freeMemory()));
    }
    
    @MessageMapping("channel")
    public Flux<Message> channel(@Payload Flux<Message> messageFlux){
        return messageFlux.doOnNext(message -> System.out.println("Sender : " + message.getSender() +
                " Receiver : " + message.getReceiver() +
                " Content : " + message.getContent()))
                .switchMap(message -> {
                    message.setContent("Hello from client2");
                    message.setSender("client2");
                    message.setReceiver("client1");
                    return Flux.just(message);
                });
    }
}
