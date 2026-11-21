SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- ============================================================================
-- sp_MarketingLeadSummary
--
-- Current channel CPL and conversion report, run nightly by the Marketing
-- Operations team ahead of the 08:00 stand-up. Reads AdHub spend, LeadFlow
-- leads and Aurora call activity. Do not run during the overnight ETL
-- window (02:00-04:00) - Leads and Calls are being reloaded.
--
-- Owner: BI Team / Marketing Operations
-- Last touched: legacy, predates source control migration
-- ============================================================================
CREATE PROCEDURE dbo.sp_MarketingLeadSummary
    @RegionCode CHAR(2) = NULL      -- optional single-region filter, NULL = all regions
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @RegionList TABLE (RegionCode CHAR(2));

    IF @RegionCode IS NOT NULL
        INSERT INTO @RegionList (RegionCode) VALUES (@RegionCode);
    ELSE
        INSERT INTO @RegionList (RegionCode)
        SELECT DISTINCT r.RegionCode
        FROM dbo.RefRegion r WITH (NOLOCK);

    DECLARE @Results TABLE (
        RegionCode      CHAR(2),
        ChannelCode     VARCHAR(30),
        TotalSpend      DECIMAL(18,2),
        TotalLeads      INT,
        TotalCalls      INT,
        ConvertedLeads  INT,
        CPL             DECIMAL(18,4),
        ConversionRate  DECIMAL(9,6)
    );

    -- Per-region, per-channel spend rollup, read once up front so the
    -- region cursor below doesn't re-scan MarketingSpendDaily on every
    -- iteration.
    CREATE TABLE #ChannelSpend (
        RegionCode  CHAR(2),
        ChannelCode VARCHAR(30),
        SpendLocal  DECIMAL(18,2)
    );

    INSERT INTO #ChannelSpend (RegionCode, ChannelCode, SpendLocal)
    SELECT
        s.RegionCode,
        s.ChannelCode,
        SUM(s.SpendLocal)
    FROM dbo.MarketingSpendDaily s WITH (NOLOCK)
    WHERE s.SpendDate >= DATEADD(day, -90, GETDATE())
    GROUP BY s.RegionCode, s.ChannelCode;

    DECLARE @CurRegion CHAR(2);

    DECLARE region_cursor CURSOR LOCAL FAST_FORWARD FOR
        SELECT RegionCode FROM @RegionList;

    OPEN region_cursor;
    FETCH NEXT FROM region_cursor INTO @CurRegion;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        INSERT INTO @Results (RegionCode, ChannelCode, TotalSpend, TotalLeads, TotalCalls, ConvertedLeads, CPL, ConversionRate)
        SELECT
            cs.RegionCode,
            cs.ChannelCode,
            cs.SpendLocal                                    AS TotalSpend,
            COUNT(DISTINCT l.LeadID)                         AS TotalLeads,
            COUNT(l.LeadID)                                  AS TotalCalls,
            SUM(CASE WHEN p.PolicyID IS NOT NULL THEN 1 ELSE 0 END) AS ConvertedLeads,
            CASE WHEN COUNT(l.LeadID) = 0 THEN NULL
                 ELSE cs.SpendLocal / COUNT(l.LeadID)
            END                                               AS CPL,
            CASE WHEN COUNT(DISTINCT l.LeadID) = 0 THEN NULL
                 ELSE CAST(SUM(CASE WHEN p.PolicyID IS NOT NULL THEN 1 ELSE 0 END) AS DECIMAL(9,6))
                      / COUNT(DISTINCT l.LeadID)
            END                                               AS ConversionRate
        FROM #ChannelSpend cs
        LEFT JOIN dbo.Leads l WITH (NOLOCK)
            ON l.ChannelCode = cs.ChannelCode
           AND l.RegionCode = cs.RegionCode
        LEFT JOIN dbo.Calls c WITH (NOLOCK)
            ON c.LeadID = l.LeadID
        LEFT JOIN dbo.Policies p WITH (NOLOCK)
            ON p.LeadID = l.LeadID
           AND p.PolicyStatus <> 'NTU'
        WHERE cs.RegionCode = @CurRegion
          AND l.LeadStatus <> 'DEAD'
        GROUP BY cs.RegionCode, cs.ChannelCode, cs.SpendLocal;

        FETCH NEXT FROM region_cursor INTO @CurRegion;
    END

    CLOSE region_cursor;
    DEALLOCATE region_cursor;

    SELECT
        RegionCode,
        ChannelCode,
        TotalSpend,
        TotalLeads,
        TotalCalls,
        ConvertedLeads,
        CPL,
        ConversionRate
    FROM @Results
    ORDER BY RegionCode, ChannelCode;

    DROP TABLE #ChannelSpend;
END
GO
