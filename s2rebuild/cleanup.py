"""s2rebuild cleanup: C5, the customer's delete of the retained original after the retention period, by its branch record.

Author: John Bergamini

After a run the workspace holds two entries, the live database and the retained copy of the
original (a resume record of the original storage, attached read only). The kit never deletes
the retained copy: no step and no configuration key does. The customer does, with this command,
after retain_until_utc (C2 records it: the switch time plus closeout.retain_days), with two
confirmations: the --delete NAME flag and the name typed again at the prompt (--yes does not
apply). Without --delete the command lists what the run left after C3, with records and dates.

A READ ONLY attachment cannot be dropped (Helios 10.8.0, fact 28), so the delete is DETACH
DATABASE, the record's own ATTACH_SYNTAX read write, a record check, DROP DATABASE, each behind
a comparison with the record the run recorded (name, STORAGE_ID, ATTACH_TYPE, COMPUTE_ID); then
the live name must still resolve to the live record. Any other outcome issues nothing and
records the event. Statements issued: DETACH DATABASE, ATTACH DATABASE and DROP DATABASE on the
retained name only, through the cleanup owner guard; SELECT on the attachment and branch views.
Every session is an owner session: C4 dropped the runner account before C5 runs.
"""
import sys

from . import ownersql, steps, switch
from .common import EXIT_DONE, KitError, iso, parse_utc, utc_now
from .sqlutil import ident, read_guard

COMMAND = 'cleanup'


def ask(prompt):
    """Read the typed name at a terminal; None without one. Tests replace this function."""
    if sys.stdin is None or not sys.stdin.isatty():
        return None
    return input(prompt).strip()


def retained_size_gib(journal):
    """The size of the retained copy in GiB from the capture the journal carries (the planned tables' primary files), or None."""
    sizes = [float(t.get('size_gib') or 0) for t in journal.tables()]
    return round(sum(sizes), 3) if sizes else None


def record_lines(ctx, names):
    """The lines of the listing: the retained record, the live record, retain_until_utc, the size and the C5 command."""
    journal = ctx.journal
    retained, live = journal.attachment('retained'), journal.attachment('live')
    lines = ['run %s, database %s' % (ctx.run_id, ctx.db.source)]
    lines.append('live:      %s' % switch.record_text(live))
    lines.append('retained:  %s' % switch.record_text(retained))
    if retained and retained.get('attach_syntax'):
        lines.append('           recovery syntax: %s' % retained['attach_syntax'])
    size = retained_size_gib(journal)
    lines.append('size:      %s' % ('%.3f GiB (primary files of the capture)' % size if size is not None else 'not captured'))
    lines.append('switched:  %s' % (journal.get_meta('switched_utc') or 'not recorded'))
    lines.append('retain until: %s (closeout.retain_days %d)' % (journal.get_meta('retain_until_utc') or 'not recorded (C2 records it)',
                                                                 ctx.config.closeout.retain_days))
    lines.append('C3 end state: %s' % (journal.step_status('C3') or 'not run'))
    listings = journal.events('endstate_listing') or journal.events('switch_listing')
    if listings:
        lines.append('last listing: %s' % listings[-1])
    lines.append('delete with: s2rebuild cleanup --delete %s%s (after the date; the name is typed again at the prompt)' % (
        ctx.db.retained, steps.run_flag(ctx.run_id)))
    return lines


def refusal(ctx, name):
    """Why the delete is refused before any read, or None: the name is not the recorded retained name, no record, before the date, C3 not passed."""
    journal = ctx.journal
    if name != ctx.db.retained:
        return 'cleanup deletes the retained copy of this run only, which is %s, not %s' % (ctx.db.retained, name)
    retained = journal.attachment('retained')
    if not retained or switch.record_key(retained) is None:
        return 'the retained record is not recorded whole in the journal (O10 S2 records it); nothing was issued'
    until = journal.get_meta('retain_until_utc')
    if not until:
        return 'retain_until_utc is not recorded (C2 records it: the switch time plus closeout.retain_days); nothing was issued'
    if utc_now() < parse_utc(until, 'retain_until_utc'):
        return 'the retention period ends at %s; the retained copy %s is kept until then; nothing was issued' % (until, name)
    if journal.step_status('C3') != 'passed':
        return 'C3 (End state) is not passed for this run; pass it first; nothing was issued'
    return None


def delete(ctx, name):
    """The C5 delete flow for the typed name; returns the note line. Every refusal and the customer's no are events, exit 0.

    Reads the live record of the name with the owner session and compares it with the journal;
    prints the record, the size, retain-until and the recovery syntax; asks for the name; then
    DETACH, the record's own ATTACH_SYNTAX read write, a record check, DROP, and the live name
    check, each behind its comparison. Raises KitError at a failed check with the observed state.
    """
    why = refusal(ctx, name)
    if why:
        ctx.journal.event('cleanup_refused', why)
        return 'cleanup refused: %s' % why
    if not ctx.owner_configured:
        raise KitError('cleanup --delete needs connections.owner in the configuration; the delete runs with the owner profile')
    names = ownersql.names_for(ctx)
    for line in record_lines(ctx, names):
        ctx.log('    ' + line)
    recorded = ctx.journal.attachment('retained')
    reader = ctx.owner_session(read_guard('owner cleanup'))
    try:
        live = switch.identify(reader, name, recorded)
    finally:
        reader.close()
    if not switch.records_match(recorded, live) or live.get('ACCESS') != 'READONLY':
        ctx.journal.event('cleanup_refused', 'the record under %s differs from the recorded retained record (expected %s; observed %s)'
                          % (name, switch.record_text(recorded), switch.record_text(live)))
        return 'cleanup refused: %s' % switch.refusal_text('the retained name holds the recorded record READONLY', recorded, live)
    ctx.log('    the retained copy %s matches the recorded record: %s' % (name, switch.record_text(live)))
    answer = ask('Type the retained name %s to delete it for good (anything else keeps it): ' % name)
    if answer is None:
        raise KitError('cleanup --delete needs a terminal to type the name %s at; --yes does not apply' % name)
    if answer != name:
        ctx.journal.event('kept_by_customer', 'cleanup --delete %s: the typed name did not match; the retained copy is kept' % name)
        return 'kept by customer: the typed name did not match %s; nothing was issued' % name
    guard = steps.guard_for(COMMAND, ctx.db, steps.OWNER)
    owner = ctx.owner_session(guard)
    try:
        # no runner session: C4 dropped the runner account before C5, and no check of this flow counts rows
        machine = switch.SwitchMachine(owner, names, ctx.journal, ctx.log, None, open_session=ctx.owner_session, guard=guard)
        ctx.journal.event('cleanup_started', 'the customer typed the retained name %s' % name)
        machine._identify('cleanup: the retained name holds the recorded record READONLY', name, recorded, ACCESS='READONLY')
        machine._run('DETACH DATABASE %s' % ident(name), 'cleanup DETACH retained')
        detached = machine._identify('cleanup: the retained record is detached with its ATTACH_SYNTAX', name, recorded, IS_DETACHED=1)
        syntax = str(detached.get('ATTACH_SYNTAX') or '').strip().rstrip(';')
        if ' AT MILESTONE ' not in syntax:
            raise KitError('cleanup stopped: the detached retained record %s shows no AT MILESTONE syntax; the name is detached and nothing is '
                           'dropped. Attach it by hand from the Portal, or by its ATTACH_SYNTAX' % switch.record_text(detached))
        machine._run(syntax, 'cleanup ATTACH retained by its ATTACH_SYNTAX')
        machine._identify('cleanup: the retained record is attached READWRITE', name, recorded, ACCESS='READWRITE', IS_DETACHED=0)
        machine._run('DROP DATABASE %s' % ident(name), 'cleanup DROP retained')
        machine._expect('cleanup: the retained record is dropped', switch.RECORD_STATE_SQL, (name, recorded['storage_id'], recorded['compute_id']),
                        ownersql.Expect('column_equals', 'IS_DROPPED', 1))
        machine._expect('cleanup: no attached database named retained', switch.ATTACHED_SQL, (name,), ownersql.Expect('no_rows'))
        machine._identify('cleanup: the live name still resolves to the live record', ctx.db.source, ctx.journal.attachment('live'), ACCESS='READWRITE')
    finally:
        owner.close()
    when = iso(utc_now())
    ctx.journal.set_meta('retained_deleted_utc', when)
    ctx.journal.event('retained_deleted', 'the customer deleted the retained copy %s (%s) at %s' % (name, switch.record_text(recorded), when))
    return 'retained copy %s deleted at %s by the customer; the live name %s still resolves to the live record' % (name, when, ctx.db.source)


def run(ctx, delete_name=None):
    """The cleanup command for one database: the listing, or the delete flow for delete_name; returns the exit code (always EXIT_DONE)."""
    names = ownersql.names_for(ctx)
    if delete_name is None:
        for line in record_lines(ctx, names):
            ctx.log('    ' + line)
        return EXIT_DONE
    if ctx.dry_run:
        ctx.log('    DRY RUN: --delete %s would compare the record, ask for the typed name and run DETACH, ATTACH by syntax, DROP; no connection '
                'was opened' % delete_name)
        return EXIT_DONE
    ctx.log('    ' + delete(ctx, delete_name))
    return EXIT_DONE
