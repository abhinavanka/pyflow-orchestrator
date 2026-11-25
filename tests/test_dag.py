import pytest

from pyflow import DAG, CycleError, DAGError


def noop():
    return None


def make_dag(edges, extra=()):
    """Build a DAG from (upstream, downstream) pairs of task names."""
    dag = DAG("test")
    names = {n for edge in edges for n in edge} | set(extra)
    tasks = {n: dag.add_task(n, noop) for n in sorted(names)}
    for up, down in edges:
        tasks[up] >> tasks[down]
    return dag


def test_linear_chain_order():
    dag = make_dag([("a", "b"), ("b", "c")])
    assert dag.topological_order() == ["a", "b", "c"]
    assert dag.topological_levels() == [["a"], ["b"], ["c"]]


def test_diamond_levels_group_parallel_tasks():
    dag = make_dag([("a", "b"), ("a", "c"), ("b", "d"), ("c", "d")])
    assert dag.topological_levels() == [["a"], ["b", "c"], ["d"]]


def test_order_respects_every_dependency():
    edges = [("a", "c"), ("b", "c"), ("c", "d"), ("a", "e"), ("e", "d")]
    dag = make_dag(edges)
    order = dag.topological_order()
    for up, down in edges:
        assert order.index(up) < order.index(down)


def test_independent_tasks_share_first_level():
    dag = make_dag([("a", "b")], extra=["z"])
    assert dag.topological_levels()[0] == ["a", "z"]


def test_cycle_is_detected():
    dag = make_dag([("a", "b"), ("b", "c"), ("c", "a")])
    with pytest.raises(CycleError) as exc:
        dag.validate()
    assert "a" in str(exc.value) and "c" in str(exc.value)


def test_cycle_does_not_blame_innocent_tasks():
    dag = make_dag([("start", "a"), ("a", "b"), ("b", "a")])
    with pytest.raises(CycleError) as exc:
        dag.validate()
    assert "start" not in str(exc.value)


def test_self_dependency_rejected():
    dag = DAG("t")
    a = dag.add_task("a", noop)
    with pytest.raises(CycleError):
        a >> a


def test_duplicate_task_id_rejected():
    dag = DAG("t")
    dag.add_task("a", noop)
    with pytest.raises(DAGError):
        dag.add_task("a", noop)


def test_empty_dag_invalid():
    with pytest.raises(DAGError):
        DAG("t").validate()


def test_non_callable_rejected():
    with pytest.raises(DAGError):
        DAG("t").add_task("a", "not a function")  # type: ignore[arg-type]


def test_cross_dag_link_rejected():
    a = DAG("one").add_task("a", noop)
    b = DAG("two").add_task("b", noop)
    with pytest.raises(DAGError):
        a >> b


def test_rshift_chain_and_lists():
    dag = DAG("t")
    a, b, c, d = (dag.add_task(n, noop) for n in "abcd")
    a >> [b, c] >> d
    assert dag.topological_levels() == [["a"], ["b", "c"], ["d"]]
    assert a.downstream == {"b", "c"}
    assert d.upstream == {"b", "c"}


def test_decorator_uses_function_name():
    dag = DAG("t")

    @dag.task()
    def extract():
        pass

    @dag.task("custom_id")
    def other():
        pass

    assert set(dag.tasks) == {"extract", "custom_id"}


def test_descendants_are_transitive():
    dag = make_dag([("a", "b"), ("b", "c"), ("a", "d")], extra=["z"])
    assert dag.descendants("a") == {"b", "c", "d"}
    assert dag.descendants("c") == set()
