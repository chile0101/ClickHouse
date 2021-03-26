from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, String, JSON, Float, DateTime

from prile.database import RMDBBase, rmdb_engine


class ExpExperiment(RMDBBase):
    __tablename__ = "exp_experiment"

    id = Column(String, primary_key=True)
    tenant_id = Column(Integer, nullable=False)
    name = Column(String)
    type = Column(String)
    target_segment_id = Column(String)
    audience_percentage = Column(String)
    test_duration = Column(Integer)
    created_by = Column(String)
    status = Column(String)
    created_at = Column(DateTime)
    updated_by = Column(String)
    winner_id = Column(String)

    # distribution = relationship('ExpVariationDistribution', secondary='exp_variation_experiment')
    # metrics = relationship('ExpMetric', secondary='exp_metric_experiment')

    def __repr__(self):
        return "<ExpExperiment(id='{}', tenant_id='{}', name={}, type={})>" \
            .format(self.id, self.tenant_id, self.name, self.type)



from prile.experiment.model.postgres_models import ExpMetric, ExpMetricExperiment

UPDATED_SUCCESS = 'updated_success'
DELETED_SUCCESS = 'deleted_success'


def _update_column(curr, new):
    if new:
        return new
    return curr


class MetricRepository:
    def __init__(self, rmdb_session):
        self.session = rmdb_session

    # # # GET # # #
    def get_metric(self, tenant_id: int, metric_id: str):
        with self.session as session:
            return session.query(ExpMetric).filter(ExpMetric.tenant_id == tenant_id,
                                                   ExpMetric.id == metric_id).first()

    def get_metrics_in_exp(self, tenant_id: int, exp_id: str):
        with self.session as session:
            return session.query(ExpMetric).join(ExpMetricExperiment,
                                                 ExpMetric.id == ExpMetricExperiment.metric_id) \
                .filter(ExpMetric.tenant_id == tenant_id,
                        ExpMetricExperiment.tenant_id == tenant_id,
                        ExpMetricExperiment.experiment_id == exp_id) \
                .all()

    # # # INSERT # # #
    def insert_metric(self, tenant_id: int, metric_id: str, metric_name: str, metric_type: str,
                      metric_formula: dict):
        with self.session as session:
            _insert_metric = ExpMetric(tenant_id=tenant_id, id=metric_id, name=metric_name, type=metric_type,
                                       formula=metric_formula)
            session.add(_insert_metric)
            session.commit()
            return _insert_metric

    def insert_metric_exp(self, tenant_id: int, exp_id: str, metric_id: str):
        with self.session as session:
            _insert_metric_exp = ExpMetricExperiment(tenant_id=tenant_id, exp_id=exp_id, metric_id=metric_id)
            session.add(_insert_metric_exp)
            session.commit()
            return _insert_metric_exp

    # # # UPDATE # # #
    def update_metric(self, tenant_id: int, metric_id: str, metric_name=None, metric_type=None,
                      metric_formula=None):
        with self.session as session:
            for c in session.query(ExpMetric).all():
                if c.tenant_id == tenant_id and c.id == metric_id:
                    c.name = _update_column(c.name, metric_name)
                    c.type = _update_column(c.type, metric_type)
                    c.formula = _update_column(c.formula, metric_formula)
                    break
            session.commit()
            return UPDATED_SUCCESS

    # # # DELETE # # #
    def delete_metric(self, tenant_id: int, metric_id: str):
        with self.session as session:
            session.query(ExpMetric).filter(ExpMetric.tenant_id == tenant_id,
                                            ExpMetric.id == metric_id).delete()
            session.commit()
            return DELETED_SUCCESS

    def delete_metric_exp(self, tenant_id: int, metric_id: str, exp_id: str):
        with self.session as session:
            session.query(ExpMetricExperiment).filter(ExpMetricExperiment.tenant_id == tenant_id,
                                                      ExpMetricExperiment.metric_id == metric_id,
                                                      ExpMetricExperiment.exp_id == exp_id).delete()
            session.commit()
            return DELETED_SUCCESS
