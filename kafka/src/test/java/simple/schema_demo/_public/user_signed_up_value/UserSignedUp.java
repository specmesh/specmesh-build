package simple.schema_demo._public.user_signed_up_value;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserSignedUp {
    String fullName;
    String email;
    int age;


}
