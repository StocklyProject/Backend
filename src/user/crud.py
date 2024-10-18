from http.client import HTTPException
from src.main import get_db_connection

def create_user(UserCreateDTO):
    database = get_db_connection()
    cursor = database.cursor()

    # 이메일 중복 처리
    cursor.execute("SELECT id FROM user WHERE email = %s", (UserCreateDTO.email,))
    if cursor.fetchone():
        raise HTTPException(status_code=400,detail="이미 존재하는 이메일입니다.")

    # 사용자 정보 저장
    # 비밀번호 및 사용자 정보 저장
    user = cursor.execute(
        "INSERT INTO user (name, email, password) VALUES (%s, %s, %s)",
        (UserCreateDTO.name, UserCreateDTO.email, UserCreateDTO.password)  # 비밀번호는 해싱 필요
    )
    database.commit()
    cursor.close()
    database.close()

    return { user }