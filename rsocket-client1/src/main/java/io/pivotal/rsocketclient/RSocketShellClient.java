package io.pivotal.rsocketclient;


import io.pivotal.rsocketclient.data.Client;
import io.pivotal.rsocketclient.data.Message;
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
import org.springframework.stereotype.Controller;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.net.URI;
import java.time.Duration;
import java.util.UUID;

@Slf4j
@ShellComponent
public class RSocketShellClient {

 
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
    public void login() {
        log.info("Connecting using client ID: {} and username: {}", Client.CLIENT_ID);
        SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new ClientHandler());
        this.rsocketRequester = rsocketRequesterBuilder
                .setupRoute("connect")
                .setupData(Client.CLIENT_ID)
                .rsocketConnector(connector -> connector.acceptor(responder))
                .websocket(URI.create("ws://localhost:7077/rsocket"));

        this.rsocketRequester.rsocketClient()
                .source()
                .doOnSuccess(success -> System.out.println("Client Connected."))
                .doOnCancel(() -> System.out.println("Client Disconnected."))
                .subscribe();
    }

    @PreDestroy
    @ShellMethod("Logout and close your connection")
    public void logout() {
        if (userIsLoggedIn()) {
            this.s();
            this.rsocketRequester.rsocketClient().dispose();
            log.info("Logged out.");
        }
    }

    private boolean userIsLoggedIn() {
        if (null == this.rsocketRequester || this.rsocketRequester.rsocketClient().isDisposed()) {
            log.info("No connection. Did you login?");
            return false;
        }
        return true;
    }

    @ShellMethod("Stream some settings to the server. Stream of responses will be printed.")
    public void channel() {
            log.info("\n\n***** Channel (bi-directional streams)\n***** Asking for a stream of messages.\n***** Type 's' to stop.\n\n");
            
            Message message = new Message(Client.CLIENT_ID, "Client2", "Hello From " + Client.CLIENT_ID);
            
            this.rsocketRequester
                    .route("personal.call")
                    .data(Flux.just(message))
                    .retrieveFlux(Message.class)
                    .doOnNext(newMessage -> System.out.println("Sender : " + newMessage.getSender() +
                            " Receiver : " + newMessage.getReceiver() +
                            " Content : " + newMessage.getContent()))
                    .subscribe();
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
    
    @MessageMapping("call")
    public Flux<Message> channel(@Payload Flux<Message> messageFlux){
        return messageFlux
            .doOnNext(message ->
                System.out.println(
                    "Sender : " + message.getSender() +
                    " Receiver : " + message.getReceiver() +
                    " Content : " + message.getContent()
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
