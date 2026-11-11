"""s2rebuild transform: the replacement-definition hook, a pass-through in this pass.

Author: John Bergamini

Section 10.21 of the architecture (by the addendum to it): the replacement differs from the
source in partition layout only. apply() returns the source definition unchanged as the
target definition, the config block databases[].target accepts an empty transform list and
refuses anything else, and report_rows() gives the "original versus target definition" block
for the workbook. A later pass fills in, in this order:

1. keys: primary, unique, shard and sort key changes per table;
2. indexes: hash and other secondary indexes added or removed;
3. storage: rowstore or columnstore choice and compression settings;
4. renames: table and column renames carried through copy, verify and views;
5. column type changes with the conversion expression used in the INSERT ... SELECT.

Statements issued: none.
"""
import dataclasses

from .common import KitError

REFUSAL = ('Table transforms are not available in this pass; the replacement differs from the source in '
           'partition layout only. Remove databases[].target.transforms or wait for the transform pass.')


@dataclasses.dataclass(frozen=True)
class TargetDefinition:
    """The original and target definitions of one table with the list of changes between them."""

    table: str
    original: str
    target: str
    changes: tuple = ()

    @property
    def identical(self):
        """True when the hook changed nothing (always, in this pass)."""
        return not self.changes and self.original == self.target


def check_target(target_spec, database=None):
    """Refuse a target block that asks for a table transform; an empty list is the only accepted value."""
    transforms = (target_spec or {}).get('transforms') or []
    if transforms:
        where = (' for database %s' % database) if database else ''
        raise KitError(REFUSAL + where)
    return True


def apply(plan, target_spec=None):
    """The pass-through hook: the target definition equals the source definition.

    plan carries name and definition (a TablePlan); target_spec is the databases[].target block.
    """
    check_target(target_spec)
    return TargetDefinition(plan.name, plan.definition, plan.definition, ())


def report_rows(targets):
    """(label, value) rows for the workbook's original versus target definition block."""
    rows = [('Replacement definition', 'partition layout only; table definitions unchanged (transform hook is a pass-through)')]
    for t in targets:
        rows.append((t.table, 'identical' if t.identical else 'changed: ' + '; '.join(t.changes)))
    return rows
