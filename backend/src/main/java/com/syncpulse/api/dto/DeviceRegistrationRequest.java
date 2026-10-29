package com.syncpulse.api.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import com.syncpulse.domain.DeviceType;

public record DeviceRegistrationRequest(
        @NotBlank String userId,
        @NotBlank String deviceId,
        @NotNull DeviceType deviceType
) {}
