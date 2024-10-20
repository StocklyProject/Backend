from pydantic import BaseModel, EmailStr, constr

class UserCreateDTO(BaseModel):
    name: constr(min_length=1, max_length=100)  # 최소 1자, 최대 100자
    email: EmailStr  # 이메일 형식으로 유효성 검증
    # 비밀번호는 최소 8자 이상, 숫자, 영어, 특수문자 하나씩 포함
    password: constr(
        min_length=8,
        regex=r"^(?=.*[A-Za-z])(?=.*\d)(?=.*[@$!%*?&])[A-Za-z\d@$!%*?&]{8,}$"
    )

    class Config:
        orm_mode = False