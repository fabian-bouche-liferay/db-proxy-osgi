package com.liferay.samples.fbo.db.proxy.datasource;

import com.liferay.portal.configuration.metatype.bnd.util.ConfigurableUtil;
import com.liferay.portal.kernel.dao.jdbc.DataSourceFactoryUtil;
import com.liferay.portal.kernel.log.Log;
import com.liferay.portal.kernel.log.LogFactoryUtil;
import com.liferay.samples.fbo.db.proxy.configuration.DBProxyDSConfiguration;

import java.sql.Connection;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Modified;

@Component(
    configurationPid = "com.liferay.samples.fbo.db.proxy.configuration.DBProxyDSConfiguration",
    service = DataSourceProvider.class, immediate = true
)
public class DataSourceProvider {

    private static final Log _log = LogFactoryUtil.getLog(DataSourceProvider.class);

    private volatile DBProxyDSConfiguration _cfg;
    private volatile DataSource _ds;

    @Activate
    @Modified
    protected void activate(Map<String, Object> props) throws Exception {
        _cfg = ConfigurableUtil.createConfigurable(DBProxyDSConfiguration.class, props);

        String jndi = normalizeJndi(_cfg.jndiName());

        try {
            Properties p = new Properties();
            p.setProperty("jndi.name", jndi);
            _ds = DataSourceFactoryUtil.initDataSource(p);
            try (Connection c = _ds.getConnection()) {
                _log.info("External DS OK: " + c.getMetaData().getURL());
            }
            return;
        } catch (Throwable t) {
            _log.warn("DataSourceFactoryUtil lookup failed for '" + jndi + "': " + t.getMessage());
        }
    }

    public DataSource getDataSource() {
        return _ds;
    }

    public String getSchema() {
        return _cfg.schema();
    }

    private String normalizeJndi(String raw) {
        if (raw == null || raw.trim().isEmpty()) return "jdbc/ProxyDS";
        return raw.trim();
    }
}