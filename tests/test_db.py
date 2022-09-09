"""
Test some DB functions
"""

from tb_db.model import Sample
from tb_db.model import CgmlstCluster


def test_cgmlst_cluster_created(db_session):
    sample = db_session.query(Sample).filter(Sample.sample_id == "S001").one()
    cluster = CgmlstCluster(cluster_id="cgmlst_cluster_01", samples=[sample])
    db_session.add(cluster)
    db_session.commit()
    cluster = db_session.query(CgmlstCluster).filter(
        CgmlstCluster.cluster_id == "cgmlst_cluster_01"
    )
    assert cluster.count() == 1
    samples = db_session.query(Sample).filter(Sample.sample_id == "S001").all()
    assert len(samples) == 2
    assert samples[0].cgmlst_cluster_id is None
    assert samples[1].cgmlst_cluster_id == cluster.one().id
