"""
Data models for representing NHTSA API resources using Pydantic.

This module defines the structure and validation rules for core data entities
like test metadata and signal metadata, ensuring data consistency and integrity.
"""

from typing import Any, Dict, Optional, Type, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


class NHTSATestMetadata(BaseModel):
    """Represents the metadata for a single NHTSA crash test.

    This model captures high-level information about a test, including the test
    number, vehicle details, and a reference to more detailed vehicle info.

    Attributes:
        test_no: The unique identifier for the test.
        test_type: The type of test conducted (e.g., "NCAP").
        make: The manufacturer of the vehicle. Defaults to "Unknown".
        model: The model of the vehicle. Defaults to "Unknown".
        model_year: The model year of the vehicle.
        vehicle_info: A dictionary with detailed vehicle data or a URL string
                      pointing to it.
    """

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    test_no: int = Field(..., alias="testNo")
    test_type: Optional[str] = Field(None, alias="testType")
    make: Optional[str] = Field("Unknown", alias="make")
    model: Optional[str] = Field("Unknown", alias="model")
    model_year: Optional[int] = Field(None, alias="modelYear")
    vehicle_info: Optional[Union[Dict[str, Any], str]] = Field(
        None, alias="vehicleInformation"
    )

    @model_validator(mode="before")
    @classmethod
    def flatten_nested_data(
        cls: Type["NHTSATestMetadata"], data: Any
    ) -> Any:
        """Flattens nested vehicle information into top-level fields.

        This validator runs before model instantiation to check if the raw
        `vehicleInformation` field is a dictionary. If it is, it extracts
        'make', 'model', and 'modelYear' and merges them into the top-level
        data structure to ensure consistent field access.

        Args:
            data: The raw input data dictionary.

        Returns:
            The modified data dictionary with flattened fields.
        """
        if isinstance(data, dict):
            v_info = data.get("vehicleInformation")

            if isinstance(v_info, dict):
                data["make"] = data.get("make") or v_info.get("make")
                data["model"] = data.get("model") or v_info.get("model")
                data["modelYear"] = (
                    data.get("modelYear")
                    or v_info.get("modelYear")
                    or v_info.get("vehicleModelYear")
                )
        return data


class SignalMetadata(BaseModel):
    """Represents metadata for a single data-acquisition channel.

    Attributes:
        channel_id: The unique identifier for the data channel.
        sensor: The type of sensor used (e.g., "Accelerometer").
        location: The location of the sensor on the vehicle.
        url: The direct download URL for the signal data file.
    """

    model_config = ConfigDict(populate_by_name=True)

    channel_id: int = Field(..., alias="channelId")
    sensor: str = Field(..., alias="sensor")
    location: str = Field(..., alias="location")
    url: str = Field(..., alias="downloadUrl")
