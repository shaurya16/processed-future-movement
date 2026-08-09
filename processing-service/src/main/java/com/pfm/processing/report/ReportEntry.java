package com.pfm.processing.report;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReportEntry(
        @JsonProperty("Client_Information") String clientInformation,
        @JsonProperty("Product_Information") String productInformation,
        @JsonProperty("Total_Transaction_Amount") long netQuantity
) {
}
