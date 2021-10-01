package io.pivotal.rsocketclient;

import io.pivotal.rsocketclient.data.Client;
import io.pivotal.rsocketclient.data.Message;
import io.rsocket.SocketAcceptor;
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
import reactor.core.publisher.Flux;

import javax.annotation.PreDestroy;
import java.net.URI;
import java.time.Duration;

@Slf4j
@ShellComponent
public class RSocketShellClient {

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
        log.info("Connecting using client ID: {}", Client.CLIENT_ID);
        SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new ClientHandler());
        this.rsocketRequester = rsocketRequesterBuilder
                .setupRoute("connect")
                .setupData(Client.CLIENT_ID)
                .rsocketConnector(connector -> connector.acceptor(responder))
                .websocket(URI.create("ws://localhost:7000/rsocket"));

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

    @ShellMethod("Send one request. Many responses (stream) will be printed.")
    public void stream() {
        if (userIsLoggedIn()) {
            log.info("\n\n**** Request-Stream\n**** Send one request.\n**** Log responses.\n**** Type 's' to stop.");
            this.rsocketRequester
                .route("stream")
                .data(new Message(Client.CLIENT_ID, "Server", "Give me File"))
                .retrieveFlux(Message.class)
                .subscribe()
            ;
        }
    }

    @ShellMethod("Stream some settings to the server. Stream of responses will be printed.")
    public void channel() {
        log.info("\n\n***** Channel (bi-directional streams)\n***** Asking for a stream of messages.\n***** Type 's' to stop.\n\n");
        
        Message message = new Message(Client.CLIENT_ID, "Client2", "Hello From " + Client.CLIENT_ID);
        
        this.rsocketRequester
            .route("personal.call")
            .data(Flux.just(message))
            .retrieveFlux(Message.class)
            .doOnNext(newMessage ->
                System.out.println(
                    "Sender : " + newMessage.getSender() +
                    "\nReceiver : " + newMessage.getReceiver() +
                    "\nContent : " + newMessage.getContent()
                )
            )
            .subscribe()
        ;
    }

    @ShellMethod("Stops Streams or Channels.")
    public void s() {
        if (userIsLoggedIn()) {
            log.info("Stopping the current stream.");
            rsocketRequester.dispose();
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
