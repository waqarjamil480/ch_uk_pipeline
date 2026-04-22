
import sys
import logging
from datetime import datetime
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import PIPELINE_MONTH
from utils.db_wrapper import DBWrapper

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_verification():
    """Run basic verification queries to sanity-check the data"""
    
    db = DBWrapper()
    
    try:
        logger.info(f"\n{'='*60}")
        logger.info(f" VERIFYING PIPELINE DATA FOR: {PIPELINE_MONTH}")
        logger.info(f"{'='*60}\n")
        
        with db.conn.cursor() as cur:
            
            # 1. Check if pipeline completed successfully
            logger.info("1. PIPELINE EXECUTION STATUS")
            cur.execute("""
                SELECT status, files_processed, files_failed, 
                       started_at, completed_at
                FROM ch_pipeline_runs 
                WHERE ch_upload = %s
                ORDER BY started_at DESC LIMIT 1
            """, (PIPELINE_MONTH,))
            
            result = cur.fetchone()
            if result:
                status, processed, failed, started, completed = result
                logger.info(f"   Status: {status}")
                logger.info(f"   Files processed: {processed}")
                logger.info(f"   Files failed: {failed}")
                logger.info(f"   Started: {started}")
                logger.info(f"   Completed: {completed}")
                if completed and started:
                    duration = completed - started
                    logger.info(f"   Duration: {duration}")
            else:
                logger.warning(f"    No pipeline run record found for {PIPELINE_MONTH}")
            
            # 2. Basic row counts
            logger.info(f"\n 2. DATA VOLUME CHECK")
            
            cur.execute("SELECT COUNT(*) FROM financials_uk_ch WHERE ch_upload = %s", (PIPELINE_MONTH,))
            financials_count = cur.fetchone()[0]
            logger.info(f"   Financial records: {financials_count:,}")
            
            cur.execute("SELECT COUNT(DISTINCT company_number) FROM financials_uk_ch WHERE ch_upload = %s", (PIPELINE_MONTH,))
            companies_count = cur.fetchone()[0]
            logger.info(f"   Unique companies: {companies_count:,}")
            
            cur.execute("SELECT COUNT(*) FROM directors_uk_ch WHERE ch_upload = %s", (PIPELINE_MONTH,))
            directors_count = cur.fetchone()[0]
            logger.info(f"   Director records: {directors_count:,}")
            
            cur.execute("SELECT COUNT(*) FROM reports_uk_ch WHERE ch_upload = %s", (PIPELINE_MONTH,))
            reports_count = cur.fetchone()[0]
            logger.info(f"   Report records: {reports_count:,}")
            
            cur.execute("SELECT COUNT(*) FROM metadata_uk_ch WHERE ch_upload = %s", (PIPELINE_MONTH,))
            metadata_count = cur.fetchone()[0]
            logger.info(f"   Metadata records: {metadata_count:,}")
            
            # Basic sanity: Should have data
            if financials_count == 0:
                logger.error("   CRITICAL: No financial records loaded!")
                return False
            else:
                logger.info("  Data loaded successfully")
            
            # 3. Check for NULL values in critical fields
            logger.info(f"\n 3. DATA QUALITY CHECK (NULL VALUES)")
            
            cur.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE company_number IS NULL) as null_company,
                    COUNT(*) FILTER (WHERE metric IS NULL) as null_metric,
                    COUNT(*) FILTER (WHERE value IS NULL) as null_value
                FROM financials_uk_ch 
                WHERE ch_upload = %s
            """, (PIPELINE_MONTH,))
            
            null_company, null_metric, null_value = cur.fetchone()
            
            if null_company > 0:
                logger.warning(f"    {null_company} records missing company_number")
            if null_metric > 0:
                logger.warning(f"    {null_metric} records missing metric")
            if null_value > 0:
                logger.warning(f"    {null_value} records missing value")
            
            if null_company == 0 and null_metric == 0 and null_value == 0:
                logger.info("    No critical NULL values found")
            
            # 4. Check for duplicate records (unique constraints)
            logger.info(f"\n 4. DUPLICATE CHECK")
            
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT company_number, director_name, COUNT(*)
                    FROM directors_uk_ch
                    WHERE ch_upload = %s
                    GROUP BY company_number, director_name
                    HAVING COUNT(*) > 1
                ) dup
            """, (PIPELINE_MONTH,))
            
            dup_directors = cur.fetchone()[0]
            if dup_directors > 0:
                logger.warning(f"    Found {dup_directors} duplicate director records")
            else:
                logger.info("    No duplicate director records")
            
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT company_number, section, COUNT(*)
                    FROM reports_uk_ch
                    WHERE ch_upload = %s
                    GROUP BY company_number, section
                    HAVING COUNT(*) > 1
                ) dup
            """, (PIPELINE_MONTH,))
            
            dup_reports = cur.fetchone()[0]
            if dup_reports > 0:
                logger.warning(f"    Found {dup_reports} duplicate report records")
            else:
                logger.info("    No duplicate report records")
            
            
            logger.info(f"\n{'='*60}")
            logger.info(f" VERIFICATION COMPLETE")
            logger.info(f"{'='*60}")
            
            # Return True if all checks passed
            return True
            # =====================================
            
    except Exception as e:
        logger.error(f" Verification failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

if __name__ == "__main__":
    success = run_verification()
    sys.exit(0 if success else 1)