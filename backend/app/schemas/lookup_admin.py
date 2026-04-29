from pydantic import BaseModel, Field


class LookupStateResponse(BaseModel):
    state_id: int
    state_name: str
    state_code: str | None = None


class LookupStateListResponse(BaseModel):
    lookup_schema: str
    runtime_schema: str
    count: int
    items: list[LookupStateResponse]


class LookupDistrictResponse(BaseModel):
    district_id: int
    district_name: str
    district_code: str | None = None
    state_id: int
    state_name: str
    state_code: str | None = None
    lookup_schema: str | None = None
    runtime_schema: str | None = None
    mirrored_to_runtime: bool | None = None


class LookupDistrictListResponse(BaseModel):
    lookup_schema: str
    runtime_schema: str
    state_id: int | None = None
    count: int
    items: list[LookupDistrictResponse]


class LookupMukimResponse(BaseModel):
    mukim_id: int
    mukim_name: str
    mukim_code: str | None = None
    district_id: int
    district_name: str
    district_code: str | None = None
    state_id: int
    state_name: str
    state_code: str | None = None
    lookup_schema: str | None = None
    runtime_schema: str | None = None
    mirrored_to_runtime: bool | None = None


class LookupMukimListResponse(BaseModel):
    lookup_schema: str
    runtime_schema: str
    state_id: int | None = None
    district_id: int | None = None
    count: int
    items: list[LookupMukimResponse]


class LookupLocalityResponse(BaseModel):
    locality_id: int
    locality_name: str
    locality_code: str | None = None
    mukim_id: int | None = None
    created_at: str | None = None
    updated_at: str | None = None
    mukim_name: str | None = None
    mukim_code: str | None = None
    district_id: int | None = None
    district_name: str | None = None
    district_code: str | None = None
    state_id: int | None = None
    state_name: str | None = None
    state_code: str | None = None
    lookup_schema: str | None = None
    runtime_schema: str | None = None
    mirrored_to_runtime: bool | None = None


class LookupLocalityListResponse(BaseModel):
    lookup_schema: str
    runtime_schema: str
    state_id: int | None = None
    district_id: int | None = None
    mukim_id: int | None = None
    count: int
    items: list[LookupLocalityResponse]


class LookupPostcodeResponse(BaseModel):
    postcode_id: int
    postcode_name: str
    postcode: str
    locality_id: int | None = None
    locality_name: str | None = None
    locality_code: str | None = None
    mukim_id: int | None = None
    mukim_name: str | None = None
    mukim_code: str | None = None
    district_id: int | None = None
    district_name: str | None = None
    district_code: str | None = None
    state_id: int | None = None
    state_name: str | None = None
    state_code: str | None = None
    lookup_schema: str | None = None
    runtime_schema: str | None = None
    mirrored_to_runtime: bool | None = None


class LookupPostcodeListResponse(BaseModel):
    lookup_schema: str
    runtime_schema: str
    state_id: int | None = None
    district_id: int | None = None
    mukim_id: int | None = None
    locality_id: int | None = None
    count: int
    items: list[LookupPostcodeResponse]


class LookupDistrictCreateRequest(BaseModel):
    district_name: str = Field(examples=["KUALA LUMPUR CENTRAL"])
    district_code: str = Field(examples=["99"])
    state_id: int = Field(examples=[14])


class LookupDistrictUpdateRequest(BaseModel):
    district_name: str | None = Field(default=None, examples=["KUALA LUMPUR CENTRAL"])
    district_code: str | None = Field(default=None, examples=["99"])
    state_id: int | None = Field(default=None, examples=[14])


class LookupMukimCreateRequest(BaseModel):
    mukim_name: str = Field(examples=["BUKIT BINTANG"])
    mukim_code: str = Field(examples=["0101"])
    district_id: int = Field(examples=[1])


class LookupMukimUpdateRequest(BaseModel):
    mukim_name: str | None = Field(default=None, examples=["BUKIT BINTANG"])
    mukim_code: str | None = Field(default=None, examples=["0101"])
    district_id: int | None = Field(default=None, examples=[1])


class LookupLocalityCreateRequest(BaseModel):
    locality_name: str = Field(examples=["JALAN BUKIT BINTANG"])
    locality_code: str | None = Field(default=None, examples=["L001"])
    mukim_id: int | None = Field(default=None, examples=[101])


class LookupLocalityUpdateRequest(BaseModel):
    locality_name: str | None = Field(default=None, examples=["JALAN BUKIT BINTANG"])
    locality_code: str | None = Field(default=None, examples=["L001"])
    mukim_id: int | None = Field(default=None, examples=[101])


class LookupPostcodeCreateRequest(BaseModel):
    postcode_name: str = Field(examples=["KUALA LUMPUR"])
    postcode: str = Field(examples=["50250"])
    locality_id: int | None = Field(default=None, examples=[1001])


class LookupPostcodeUpdateRequest(BaseModel):
    postcode_name: str | None = Field(default=None, examples=["KUALA LUMPUR"])
    postcode: str | None = Field(default=None, examples=["50250"])
    locality_id: int | None = Field(default=None, examples=[1001])
