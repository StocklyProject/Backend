from fastapi import APIRouter
from src.user.schemas import UserCreateDTO
from src.main import get_db_connection
from src.user.crud import create_user
router = APIRouter(
    prefix="/api/v1/users",
    tags=["users"],
)

@router.post('/signup')
async def signup(userdata: UserCreateDTO):
    user = create_user(userdata)
    return user


@router.post('/login')
async def login():

@router.delete('')
async def delete_user():

@router.post('/logout')
async def logout():

@router.get('')
async def get_user():
