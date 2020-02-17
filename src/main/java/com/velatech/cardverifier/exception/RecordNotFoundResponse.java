package com.velatech.cardverifier.exception;


import lombok.Data;

@Data
public class recordNotFoundResponse {

    private String message;

    recordNotFoundResponse(String message){
        this.message = message;
    }
}
