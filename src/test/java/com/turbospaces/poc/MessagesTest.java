package com.turbospaces.poc;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turbospaces.poc.Messages.UserCommand;

public class MessagesTest {
    Logger logger = LoggerFactory.getLogger( getClass() );

    @Test
    public void json() throws IOException {
        String json = Misc.mapper.writeValueAsString( UserCommand.some( 1 ) );
        logger.debug( json );
        logger.debug( Misc.mapper.readValue( json, Messages.class ).toString() );
    }
}
