from pydantic import BaseModel, Field


class AgencyTokenRequest(BaseModel):
    grant_type: str | None = Field(default=None, examples=["client_credentials"])
    client_id: str | None = Field(default=None, examples=["ak_2f6c4df54a6b8f10"])
    client_secret: str | None = Field(default=None, examples=["vx8Kx7e5d7w1w8x2f..."])
    expires_in: int | None = Field(default=None, ge=60, le=86400, examples=[3600])


class UserLoginRequest(BaseModel):
    username: str = Field(min_length=1, examples=["noraini.ahmad"])
    password: str = Field(min_length=1, examples=["Secur3Pass!"])
    expires_in: int | None = Field(default=None, ge=60, le=86400, examples=[3600])


class AgencyTokenResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    agency_id: str
    role: str | None = None
    auth_type: str | None = None
    user_id: str | None = None
    username: str | None = None
