package com.dataextraction.datapipelines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Mints short-lived HS256 JWTs for authenticating with the datapipelines service.
 * The shared secret is the raw UTF-8 bytes of {@code DATAPIPELINES_JWT_SECRET_KEY}
 * (must be at least 32 bytes for HS256), matching how the service reads it.
 */
public final class JwtTokenProvider {

    private static final String HMAC_ALGORITHM = "HmacSHA256";
    private static final int MIN_SECRET_BYTES = 32;
    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();
    private static final String HEADER_JSON = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";

    private final ObjectMapper objectMapper;
    private final byte[] secret;
    private final String subject;
    private final Duration tokenTtl;
    private final boolean jwtEnabled;

    public JwtTokenProvider(String secret, String subject, Duration tokenTtl) {
        this(secret, subject, tokenTtl, false);
    }

    public JwtTokenProvider(String secret, String subject, Duration tokenTtl, boolean jwtEnabled) {
        this.jwtEnabled = jwtEnabled;
        if (jwtEnabled) {
            this.objectMapper = new ObjectMapper();
            Objects.requireNonNull(secret, "secret");
            byte[] secretBytes = secret.getBytes(StandardCharsets.UTF_8);
            if (secretBytes.length < MIN_SECRET_BYTES) {
                throw new IllegalArgumentException(
                        "JWT secret must be at least " + MIN_SECRET_BYTES + " bytes for HS256; got "
                                + secretBytes.length);
            }
            this.secret = secretBytes;
            this.subject = Objects.requireNonNull(subject, "subject");
            this.tokenTtl = Objects.requireNonNull(tokenTtl, "tokenTtl");
        }
        else {
            this.objectMapper = null;
            this.secret = null;
            this.subject = null;
            this.tokenTtl = null;
        }
    }

    /**
     * Returns a freshly signed compact JWT valid for {@code tokenTtl} from now.
     */
    public String mintToken() {
        if (!isJwtEnabled()) {
            return null;
        } else {
            Instant now = Instant.now();
            Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("sub", subject);
            claims.put("iat", now.getEpochSecond());
            claims.put("exp", now.plus(tokenTtl).getEpochSecond());

            String headerSegment = base64Url(HEADER_JSON.getBytes(StandardCharsets.UTF_8));
            String payloadSegment = base64Url(toJson(claims));
            String signingInput = headerSegment + "." + payloadSegment;
            String signatureSegment = base64Url(sign(signingInput.getBytes(StandardCharsets.UTF_8)));
            return signingInput + "." + signatureSegment;
        }
    }



    private byte[] toJson(Map<String, Object> claims) {
        try {
            return objectMapper.writeValueAsBytes(claims);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize JWT claims", e);
        }
    }

    private byte[] sign(byte[] signingInput) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGORITHM);
            mac.init(new SecretKeySpec(secret, HMAC_ALGORITHM));
            return mac.doFinal(signingInput);
        } catch (java.security.GeneralSecurityException e) {
            throw new IllegalStateException("Failed to sign JWT", e);
        }
    }

    private static String base64Url(byte[] data) {
        return BASE64_URL.encodeToString(data);
    }

    public boolean isJwtEnabled() {
        return jwtEnabled;
    }
}
