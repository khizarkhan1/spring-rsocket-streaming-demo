package com.example.rsocketclient2.data;

import lombok.Data;
import lombok.NoArgsConstructor;

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