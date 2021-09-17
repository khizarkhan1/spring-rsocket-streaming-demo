package io.pivotal.rsocketserver.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
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