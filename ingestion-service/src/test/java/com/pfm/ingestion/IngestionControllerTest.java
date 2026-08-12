package com.pfm.ingestion;

import com.pfm.common.fixedwidth.ParseError;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(IngestionController.class)
class IngestionControllerTest {

    @Autowired
    MockMvc mockMvc;

    @MockitoBean
    IngestionService ingestionService;

    @MockitoBean
    IngestionStatusService statusService;

    @Test
    void postIngestReturnsResultFromService() throws Exception {
        IngestionResult result = new IngestionResult(
                "fp-1", 3, 2, 1, List.of(new ParseError(3, "bad", "too short")), false);
        when(ingestionService.ingest(false)).thenReturn(result);

        mockMvc.perform(post("/api/v1/ingest"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.fingerprint").value("fp-1"))
                .andExpect(jsonPath("$.totalLines").value(3))
                .andExpect(jsonPath("$.published").value(2))
                .andExpect(jsonPath("$.skipped").value(1))
                .andExpect(jsonPath("$.cached").value(false));

        verify(ingestionService).ingest(false);
    }

    @Test
    void postIngestWithForceParamPassesForceFlagThrough() throws Exception {
        IngestionResult result = new IngestionResult("fp-1", 3, 3, 0, List.of(), false);
        when(ingestionService.ingest(true)).thenReturn(result);

        mockMvc.perform(post("/api/v1/ingest").param("force", "true"))
                .andExpect(status().isOk());

        verify(ingestionService).ingest(true);
    }

    @Test
    void kafkaPublishFailureReturns502() throws Exception {
        when(ingestionService.ingest(false)).thenThrow(new KafkaPublishException("all failed", List.of()));

        mockMvc.perform(post("/api/v1/ingest"))
                .andExpect(status().isBadGateway());
    }

    @Test
    void missingFileReturns404() throws Exception {
        when(ingestionService.ingest(false))
                .thenThrow(new IngestionFileNotFoundException(Path.of("/some/deep/absolute/missing.txt")));

        mockMvc.perform(post("/api/v1/ingest"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("Ingestion file not found"))
                .andExpect(jsonPath("$.error", org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("/some/deep/absolute/missing.txt"))));
    }

    @Test
    void statusEndpointReturns200WithTheConfiguredPath() throws Exception {
        when(statusService.currentStatus()).thenReturn(new IngestionStatus(
                "sample-data/Input.txt", true, 127624L,
                Instant.parse("2026-08-12T09:14:00Z"),
                Instant.parse("2026-08-12T14:31:52Z"), "fp-1", 717, 717, 0, 0));

        mockMvc.perform(get("/api/v1/ingest/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.configuredPath").value("sample-data/Input.txt"))
                .andExpect(jsonPath("$.published").value(717));
    }
}
