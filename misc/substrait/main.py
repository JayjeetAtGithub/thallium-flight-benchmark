from datetime import date

import ibis
import ibis.expr.datatypes as dt

from ibis_substrait.compiler.core import SubstraitCompiler

if __name__ == "__main__":
    table = ibis.table(
        [
            ("l_orderkey", dt.int64),
            ("l_partkey", dt.int64),
            ("l_suppkey", dt.int64),
            ("l_linenumber", dt.int64),
            ("l_quantity", dt.Decimal(15, 2)),
            ("l_extendedprice", dt.Decimal(15, 2)),
            ("l_discount", dt.Decimal(15, 2)),
            ("l_tax", dt.Decimal(15, 2)),
            ("l_returnflag", dt.string),
            ("l_linestatus", dt.string),
            ("l_shipdate", dt.date),
            ("l_commitdate", dt.date),
            ("l_receiptdate", dt.date),
            ("l_shipinstruct", dt.string),
            ("l_shipmode", dt.string),
            ("l_comment", dt.string),
        ],
        name="lineitem",
    )

    query = (
        table.filter(lambda t: t.l_shipdate <= date(year=1998, month=9, day=2))
        .group_by(["l_returnflag", "l_linestatus"])
        .aggregate(
            sum_qty=lambda t: t.l_quantity.sum(),
            sum_base_price=lambda t: t.l_extendedprice.sum(),
            sum_disc_price=lambda t: (t.l_extendedprice * (1 - t.l_discount)).sum(),
            sum_charge=lambda t: (
                t.l_extendedprice * (1 - t.l_discount) * (1 + t.l_tax)
            ).sum(),
            avg_qty=lambda t: t.l_quantity.mean(),
            avg_price=lambda t: t.l_extendedprice.mean(),
            avg_disc=lambda t: t.l_discount.mean(),
            count_order=lambda t: t.count(),
        )
        .sort_by(["l_returnflag", "l_linestatus"])
    )

    compiler = SubstraitCompiler()
    plan = compiler.compile(query)
    print(plan)
    print(plan.SerializeToString())
