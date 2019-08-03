import com.test.spark.CampaignMetricsJava;
import junit.framework.TestCase;
import org.junit.Test;

public class CampaignTestJava extends TestCase {

    private com.test.spark.CampaignMetricsJava CampaignMetricsJava = null;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        CampaignMetricsJava = new CampaignMetricsJava();
    }

    @Override
    protected void tearDown() throws Exception {
        CampaignMetricsJava.spark.stop();
        super.tearDown();
    }

    @Test
    public void testCampaignDataContains4Rows() {
        assertEquals(CampaignMetricsJava.explodedCampaignDf().count(), 4);
    }

    public void testdenormarnalized10Rows() {
        assertEquals(CampaignMetricsJava.campaignDealsProductDf().count(), 10);
    }

    public void testactiveDealsActualPrice8Rows() {
        assertEquals(CampaignMetricsJava.activeDealsActualPriceChannel().count(), 8);
    }

    public void testtotalTransactionTotalAmount4Rows() {
        assertEquals(CampaignMetricsJava.totalTransactionTotalAmountActive().count(), 4);
    }

    public void testlatestTxnDate2Rows() {
        assertEquals(CampaignMetricsJava.dealwiseLatestTxnData().count(), 2);
    }
}
