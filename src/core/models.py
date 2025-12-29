from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import Optional, Any


class NHTSATestMetadata(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    test_no: int = Field(..., alias="testNo")
    test_type: Optional[str] = Field(None, alias="testType")
    make: Optional[str] = Field("Unknown", alias="make")
    model: Optional[str] = Field("Unknown", alias="model")
    model_year: Optional[int] = Field(None, alias="modelYear")
    report_url: Optional[str] = Field(None, alias="reportUrl")

    @model_validator(mode="before")
    @classmethod
    def flatten_nested_data(cls, data: Any) -> Any:
        if isinstance(data, dict):
            v = data.get("vehicle")
            if isinstance(v, dict):
                data["make"] = data.get("make") or v.get("make")
                data["model"] = data.get("model") or v.get("model")
                data["modelYear"] = data.get("modelYear") or v.get("modelYear")
        return data


class SignalMetadata(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    channel_id: int = Field(..., alias="channelId")
    sensor: str = Field(..., alias="sensor")
    location: str = Field(..., alias="location")
    url: str = Field(..., alias="downloadUrl")
