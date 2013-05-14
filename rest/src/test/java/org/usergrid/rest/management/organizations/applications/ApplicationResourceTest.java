package org.usergrid.rest.management.organizations.applications;

import org.codehaus.jackson.JsonNode;
import org.junit.Test;
import org.usergrid.cassandra.CassandraRunner;
import org.usergrid.management.ApplicationInfo;
import org.usergrid.management.UserInfo;
import org.usergrid.persistence.EntityManager;
import org.usergrid.persistence.EntityManagerFactory;
import org.usergrid.persistence.cassandra.CassandraService;
import org.usergrid.persistence.entities.User;
import org.usergrid.rest.AbstractRestTest;

import javax.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.usergrid.utils.MapUtils.hashMap;

/**
 * @author zznate
 */
public class ApplicationResourceTest extends AbstractRestTest {

  private EntityManagerFactory emf = CassandraRunner.getBean(EntityManagerFactory.class);

  @Test
  public void testSiaProviderSetup() throws Exception {
    Map<String, String> payload = hashMap("email",
            "test-sia-provider@mockserver.com").map("username", "test-sia-provider")
            .map("name", "Test User").map("password", "password")
            .map("organization", "test-sia-provider");


    JsonNode node = resource().path("/management/organizations")
            .accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .post(JsonNode.class, payload);

    node = resource().path("/management/me").queryParam("grant_type", "password")
            .queryParam("username", "test-sia-provider").queryParam("password", "password")
            .accept(MediaType.APPLICATION_JSON).get(JsonNode.class);

    logNode(node);
    String token = node.get("access_token").getTextValue();

    assertNotNull(token);

    payload = hashMap("name", "providerapp");

    logNode(node);

    node = resource().path("/management/orgs/test-sia-provider/apps")
            .queryParam("access_token", token)
                .accept(MediaType.APPLICATION_JSON)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(JsonNode.class, payload);

    logNode(node);



    payload = hashMap("api_url", "http://localhost/some/url")
            .map("client_id","123")
            .map("client_secret","4567");


    node = resource().path("/management/orgs/test-sia-provider/apps/providerapp/sia-provider")
            .queryParam("access_token", token)
            .queryParam("provider_key","pingident")
            .accept(MediaType.APPLICATION_JSON)
            .type(MediaType.APPLICATION_JSON_TYPE)
            .post(JsonNode.class, payload);

    logNode(node);
  }
}
