package com.example.demo.models;

import java.util.Date;

import org.bson.BsonDocument;
import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"id"})
@ToString
public class ResumeToken {

    @Id
    private ObjectId id;
    private int threadID;
    private BsonDocument resumeToken;
    private Date date;
    private String appName;

    public ResumeToken(int threadID, BsonDocument resumeToken, Date date, String appName) {
        this.threadID = threadID;
        this.resumeToken = resumeToken;
        this.date = date;
        this.appName = appName;
    }
}
