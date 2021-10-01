package io.pivotal.rsocketserver.data;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.awt.image.DataBuffer;
import java.nio.ByteBuffer;
import java.time.Instant;

@Data
@Slf4j
@NoArgsConstructor
@Setter
@Getter
public class Message {
    
    private String sender;
    private String receiver;
    private String content;
    
    public Message(String sender, String receiver, String content){
        this.sender = sender;
        this.receiver = receiver;
        this.content = content;
    }
    
}