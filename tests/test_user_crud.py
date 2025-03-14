import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from easyminer.crud.user import (
    get_user,
    get_user_by_email,
    get_users,
)
from easyminer.models.user import User


@pytest.mark.asyncio
async def test_get_users_empty(db_session: AsyncSession):
    """Test getting all users when there are none."""
    users = await get_users(db_session)
    assert len(users) == 0


@pytest.mark.asyncio
async def test_get_users(db_session: AsyncSession):
    """Test getting all users."""
    # Create some test users
    user1 = User(
        username="user1",
        slug="user1",
        email="user1@example.com",
        first_name="User",
        last_name="One",
        hashed_password="hashed1",
        is_superuser=False,
    )
    user2 = User(
        username="user2",
        slug="user2",
        email="user2@example.com",
        first_name="User",
        last_name="Two",
        hashed_password="hashed2",
        is_superuser=False,
    )
    db_session.add_all([user1, user2])
    await db_session.commit()

    # Get all users
    users = await get_users(db_session)
    assert len(users) == 2
    assert any(user.email == "user1@example.com" for user in users)
    assert any(user.email == "user2@example.com" for user in users)


@pytest.mark.asyncio
async def test_get_user(db_session: AsyncSession):
    """Test getting a user by ID."""
    # Create a test user
    user = User(
        username="testget",
        slug="testget",
        email="get_user_test@example.com",
        first_name="Test",
        last_name="User",
        hashed_password="hashed_pwd",
        is_superuser=False,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)

    # Get the user by ID
    retrieved_user = await get_user(db_session, user.id)
    assert retrieved_user is not None
    assert retrieved_user.id == user.id
    assert retrieved_user.email == "get_user_test@example.com"
    assert retrieved_user.first_name == "Test"
    assert retrieved_user.last_name == "User"


@pytest.mark.asyncio
async def test_get_user_not_found(db_session: AsyncSession):
    """Test getting a user that doesn't exist."""
    # Try to get a user with a non-existent ID
    with pytest.raises(HTTPException) as excinfo:
        await get_user(db_session, 999)

    # Check the exception details
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "User not found"


@pytest.mark.asyncio
async def test_get_user_by_email(db_session: AsyncSession):
    """Test getting a user by email."""
    # Create a test user
    email = "email_test@example.com"
    user = User(
        username="emailtest",
        slug="emailtest",
        email=email,
        first_name="Email",
        last_name="Test",
        hashed_password="hashed_pwd",
        is_superuser=False,
    )
    db_session.add(user)
    await db_session.commit()

    # Get the user by email
    retrieved_user = await get_user_by_email(db_session, email)
    assert retrieved_user is not None
    assert retrieved_user.id == user.id
    assert retrieved_user.email == email
    assert retrieved_user.first_name == "Email"
    assert retrieved_user.last_name == "Test"


@pytest.mark.asyncio
async def test_get_user_by_email_not_found(db_session: AsyncSession):
    """Test getting a user by an email that doesn't exist."""
    # Try to get a user with a non-existent email
    user = await get_user_by_email(db_session, "nonexistent@example.com")
    assert user is None


@pytest.mark.asyncio
async def test_email_uniqueness(db_session: AsyncSession):
    """Test that email addresses must be unique."""
    # Create a user
    email = "duplicate@example.com"
    user1 = User(
        username="duplicate1",
        slug="duplicate1",
        email=email,
        first_name="First",
        last_name="User",
        hashed_password="hashed1",
        is_superuser=False,
    )
    db_session.add(user1)
    await db_session.commit()

    # Try to create another user with the same email
    user2 = User(
        username="duplicate2",
        slug="duplicate2",
        email=email,
        first_name="Second",
        last_name="User",
        hashed_password="hashed2",
        is_superuser=False,
    )
    db_session.add(user2)

    # This should raise an exception due to the unique constraint on email
    with pytest.raises(Exception) as excinfo:
        await db_session.commit()

    # Verify the exception is related to a uniqueness constraint
    assert "UNIQUE constraint failed" in str(excinfo.value)
