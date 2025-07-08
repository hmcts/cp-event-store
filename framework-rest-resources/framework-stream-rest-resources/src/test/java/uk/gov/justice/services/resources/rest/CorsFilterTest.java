package uk.gov.justice.services.resources.rest;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.core.MultivaluedHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CorsFilterTest {

    private CorsFilter corsFilter;

    @Mock
    private ContainerRequestContext requestContext;

    @Mock
    private ContainerResponseContext responseContext;

    @BeforeEach
    void setUp() {
        corsFilter = new CorsFilter();
    }

    @Test
    void shouldAddCorsHeaders() throws Exception {
        final MultivaluedHashMap<String, Object> headers = new MultivaluedHashMap<>();
        when(responseContext.getHeaders()).thenReturn(headers);

        corsFilter.filter(requestContext, responseContext);

        assertThat(headers.get("Access-Control-Allow-Origin").get(0), is("*"));
    }
}