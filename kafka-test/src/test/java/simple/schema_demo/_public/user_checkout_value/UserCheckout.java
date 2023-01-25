package simple.schema_demo._public.user_checkout_value;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject;
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaString;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonSchemaInject(strings = {
        @JsonSchemaString(path = "javaType", value = "simple.schema_demo._public.user_checkout_value.UserCheckout")})
public class UserCheckout {
    @JsonProperty
    long id;
    @JsonProperty
    String name;
    @JsonProperty
    int price;
    @JsonProperty
    String systemDate;
}
