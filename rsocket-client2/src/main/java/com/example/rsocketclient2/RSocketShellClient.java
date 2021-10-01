package com.example.rsocketclient2;

import com.example.rsocketclient2.data.Client;
import com.example.rsocketclient2.data.Message;
import io.rsocket.SocketAcceptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import reactor.core.publisher.Flux;
import javax.annotation.PreDestroy;
import java.net.URI;


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
                .websocket(URI.create("ws://localhost:7077/rsocket"));
        ;

        this.rsocketRequester.rsocketClient().source()
                .doOnSuccess(success -> System.out.println("Client Connected."))
                .doOnCancel(() -> System.out.println("Client Disconnected."))
                .subscribe()
        ;
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
        
        Message message = new Message(Client.CLIENT_ID, "Client1", "Hello From " + Client.CLIENT_ID);

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
    
        @ShellMethod("Send one request. Many responses (stream) will be printed.")
    public void stream() {
        if (userIsLoggedIn()) {
            log.info("\n\n**** Request-Stream\n**** Send one request.\n**** Log responses.\n**** Type 's' to stop.");
            this.rsocketRequester
                    .route("stream")
                    .data(new Message(Client.CLIENT_ID, "Server", "Give me File"))
                    .retrieveFlux(Message.class)
                    .subscribe(message -> log.info("Response: {} \n(Type 's' to stop.)", message));
        }
    }
}