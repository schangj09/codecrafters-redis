package org.baylight.redis.commands;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.assertj.core.api.WithAssertions;
import org.baylight.redis.RedisServiceBase;
import org.baylight.redis.StoredData;
import org.baylight.redis.commands.RedisCommand.Type;
import org.baylight.redis.protocol.RespBulkString;
import org.baylight.redis.protocol.RespConstants;
import org.baylight.redis.protocol.RespValue;

// Note: Generated by CodiumAI
// However, I had to do some work to add verifications.

import org.junit.jupiter.api.Test;

public class SetCommandTest implements WithAssertions {
    RespBulkString SET = new RespBulkString("set".getBytes());

    // SetCommand can be instantiated without arguments
    @Test
    public void test_instantiation_without_arguments() {
        // given

        // when
        SetCommand setCommand = new SetCommand();

        // then
        assertThat(setCommand).isNotNull();
        assertThat(setCommand.getType()).isEqualTo(Type.SET);
    }

    // SetCommand can be instantiated with a key and a value
    @Test
    public void test_instantiation_with_key_and_value() {
        // given
        RespBulkString key = new RespBulkString("mykey".getBytes());
        RespBulkString value = new RespBulkString("myvalue".getBytes());

        // when
        SetCommand setCommand = new SetCommand(key, value);

        // then
        assertThat(setCommand).isNotNull();
        assertThat(setCommand.getType()).isEqualTo(Type.SET);
        assertThat(setCommand.getKey()).isEqualTo(key);
        assertThat(setCommand.getValue()).isEqualTo(value);
    }

    // SetCommand can set a key-value pair
    @Test
    public void test_set_key_value_pair() {
        // given
        RedisServiceBase service = mock(RedisServiceBase.class);
        SetCommand setCommand = new SetCommand();
        RespBulkString key = new RespBulkString("mykey".getBytes());
        RespBulkString value = new RespBulkString("myvalue".getBytes());
        setCommand.setArgs(new RespValue[] { SET, key, value });

        // when
        byte[] result = setCommand.execute(service);

        // then
        verify(service).set(eq("mykey"), any(StoredData.class));
        assertThat(result).isEqualTo(RespConstants.OK);
    }

    // SetCommand can set a key-value pair and return OK
    @Test
    public void test_set_key_value_pair_and_return_ok() {
        // given
        RedisServiceBase service = mock(RedisServiceBase.class);
        SetCommand setCommand = new SetCommand();
        RespBulkString key = new RespBulkString("mykey".getBytes());
        RespBulkString value = new RespBulkString("myvalue".getBytes());
        setCommand.setArgs(new RespValue[] { SET, key, value });
        when(service.containsKey("mykey")).thenReturn(false);

        // when
        byte[] result = setCommand.execute(service);

        // then
        verify(service).set(eq("mykey"), any(StoredData.class));
        assertThat(result).isEqualTo(RespConstants.OK);
    }

    // SetCommand throws an IllegalArgumentException if a required argument is missing
    @Test
    public void test_throw_exception_if_required_argument_missing() {
        // given
        SetCommand setCommand = new SetCommand();
        RespValue[] args = { SET, new RespBulkString("mykey".getBytes()) };
    
        // when
        Throwable throwable = catchThrowable(() -> setCommand.setArgs(args));

        // then
        assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
        assertThat(throwable.getMessage())
                .isEqualTo("SET: Missing required arg '' at index 2");
    }

    // SetCommand throws an IllegalArgumentException if an unrecognized argument is passed
    @Test
    public void test_throw_exception_if_unrecognized_argument_passed() {
        // given
        SetCommand setCommand = new SetCommand();
        RespValue[] args = { SET, new RespBulkString("mykey".getBytes()),
                new RespBulkString("myvalue".getBytes()),
                new RespBulkString("unknown".getBytes()) };
        ;

        // when
        Throwable throwable = catchThrowable(() -> setCommand.setArgs(args));

        // then
        assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
        assertThat(throwable.getMessage()).isEqualTo("SET: unrecognized arg at index 3, BulkString [length=7, value=unknown]");
    }
}