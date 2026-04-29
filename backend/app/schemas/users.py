from pydantic import BaseModel, Field


class AgencyUserCreateRequest(BaseModel):
    username: str = Field(min_length=1, examples=["noraini.ahmad"])
    password: str = Field(min_length=8, examples=["Secur3Pass!"])
    role: str = Field(default="agency_operator", examples=["agency_operator"])
    display_name: str | None = Field(default=None, examples=["Noraini Ahmad"])
    email: str | None = Field(default=None, examples=["noraini.ahmad@agency.gov.my"])


class AgencyUserResponse(BaseModel):
    user_id: str
    agency_id: str
    username: str
    display_name: str | None = None
    email: str | None = None
    role: str
    status: str
    created_by: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    last_login_at: str | None = None


class AgencyUserListResponse(BaseModel):
    count: int
    items: list[AgencyUserResponse]
