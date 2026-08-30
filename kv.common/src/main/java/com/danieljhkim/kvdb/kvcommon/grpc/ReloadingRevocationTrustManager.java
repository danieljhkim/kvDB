package com.danieljhkim.kvdb.kvcommon.grpc;

import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.HexFormat;
import java.util.Locale;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import javax.net.ssl.X509TrustManager;

/** Trust-bundle validation with a revocation file that is reloaded for every handshake. */
final class ReloadingRevocationTrustManager extends X509ExtendedTrustManager {

    private final X509TrustManager delegate;
    private final Path revocationList;

    static ReloadingRevocationTrustManager create(Path trustBundle, Path revocationList) {
        try (InputStream input = Files.newInputStream(trustBundle)) {
            CertificateFactory certificates = CertificateFactory.getInstance("X.509");
            KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
            store.load(null);
            int index = 0;
            for (var certificate : certificates.generateCertificates(input)) {
                store.setCertificateEntry("kvdb-ca-" + index++, certificate);
            }
            if (index == 0) {
                throw new CertificateException("Trust bundle contains no certificates");
            }
            TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            factory.init(store);
            for (var manager : factory.getTrustManagers()) {
                if (manager instanceof X509TrustManager x509TrustManager) {
                    return new ReloadingRevocationTrustManager(x509TrustManager, revocationList);
                }
            }
            throw new CertificateException("No X.509 trust manager is available");
        } catch (Exception e) {
            throw new IllegalStateException("Unable to load gRPC TLS trust bundle: " + trustBundle, e);
        }
    }

    private ReloadingRevocationTrustManager(X509TrustManager delegate, Path revocationList) {
        this.delegate = delegate;
        this.revocationList = revocationList;
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
        checkRevoked(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
        checkRevoked(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        if (delegate instanceof X509ExtendedTrustManager extended) {
            extended.checkClientTrusted(chain, authType, socket);
        } else {
            delegate.checkClientTrusted(chain, authType);
        }
        checkRevoked(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
            throws CertificateException {
        if (delegate instanceof X509ExtendedTrustManager extended) {
            extended.checkServerTrusted(chain, authType, socket);
        } else {
            delegate.checkServerTrusted(chain, authType);
        }
        checkRevoked(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        if (delegate instanceof X509ExtendedTrustManager extended) {
            extended.checkClientTrusted(chain, authType, engine);
        } else {
            delegate.checkClientTrusted(chain, authType);
        }
        checkRevoked(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
            throws CertificateException {
        if (delegate instanceof X509ExtendedTrustManager extended) {
            extended.checkServerTrusted(chain, authType, engine);
        } else {
            delegate.checkServerTrusted(chain, authType);
        }
        checkRevoked(chain);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }

    private void checkRevoked(X509Certificate[] chain) throws CertificateException {
        if (revocationList == null || chain == null || chain.length == 0) {
            return;
        }
        try {
            String fingerprint = HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(chain[0].getEncoded()));
            try (var lines = Files.lines(revocationList, StandardCharsets.UTF_8)) {
                boolean revoked = lines.map(String::trim)
                        .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                        .map(line -> line.replace(":", "").toLowerCase(Locale.ROOT))
                        .anyMatch(fingerprint::equals);
                if (revoked) {
                    throw new CertificateException("Peer certificate is revoked");
                }
            }
        } catch (CertificateException e) {
            throw e;
        } catch (Exception e) {
            throw new CertificateException("Unable to read the certificate revocation list", e);
        }
    }
}
