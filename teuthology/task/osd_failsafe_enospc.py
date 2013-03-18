from cStringIO import StringIO
import logging
import ceph_manager
from teuthology import misc as teuthology
import time
from ..orchestra import run

log = logging.getLogger(__name__)


def rados(testdir, remote, cmd, wait=True):
    log.info("rados %s" % ' '.join(cmd))
    pre = [
        '{tdir}/enable-coredump'.format(tdir=testdir),
        'ceph-coverage',
        '{tdir}/archive/coverage'.format(tdir=testdir),
        'rados',
        ];
    pre.extend(cmd)
    proc = remote.run(
        args=pre,
        check_status=False,
        wait=wait
        )
    if wait:
        return proc.exitstatus
    else:
        return proc

def task(ctx, config):
    """
    Test handling of osd_failsafe_nearfull_ratio and osd_failsafe_full_ratio
    configuration settings

    config: none

    """
    if config is None:
        config = {}
    assert isinstance(config, dict), \
        'osd_failsafe_enospc task only accepts a dict for configuration'
    first_mon = teuthology.get_first_mon(ctx, config)
    (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()

    testdir = teuthology.get_testdir(ctx)
    manager = ceph_manager.CephManager(
        mon,
        ctx=ctx,
        logger=log.getChild('ceph_manager'),
        )
    ctx.manager = manager

    # Give 2 seconds for injectargs + osd_op_complaint_time (30) + 2 * osd_heartbeat_interval (6) + 6 padding
    sleep_time = 50

    # XXX: Wait for osd.0 to be up?
    while len(manager.get_osd_status()['up']) < 6:
        time.sleep(10)

    # State NONE -> NEAR
    log.info('1. Verify warning messages when exceeding nearfull_ratio')

    proc = mon.run(
        args=[
          'ceph', '-w', '-t', str(sleep_time),
        ],
        stdout=StringIO(),
        wait=False,
        )

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_nearfull_ratio .00001')

    proc.exitstatus.get()

    lines = proc.stdout.getvalue().split('\n')
    print lines

    count = len(filter(lambda line: '[WRN] OSD near full' in line, lines))
    assert count == 2, 'Incorrect number of warning messages expected 2 got %d' % count

    """
    proc = mon.run(args=['ceph', '-w'], stdout=PIPE, wait=False)

    #Should see "[WRN] OSD near full" twice in ceph -w output in 50 seconds
    count = 0
    error = 0
    while True:
        line = proc.stdout.readline()
        if line.contains("[WRN] OSD near full"):
            count++
        if line.contains("OSD full"):
            log.error('incorrect log message: %s', line)
            error++

    if count != 2:
        log.error('Incorrect number of warning messages expected 2 got %d', count)
        error++

    # Terminate ceph -w

    # State NEAR -> FULL
    log.info('2. Verify error messages when exceeding full_ratio')

    proc = remote.run(args=['ceph', '-w'], stdout=PIPE, wait=False)

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .00001')
    time.sleep(10)

    # Check log message

    log.info('3. Verify write failure when exceeding full_ratio')

    # Write data

    # Put back default
    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .97')
    time.sleep(10)

    # State FULL -> NEAR
    log.info('4. Verify write success when NOT exceeding full_ratio')

    # Write should succeed

    log.info('5. Verify warning messages again when exceeding nearfull_ratio')

    # Check warning messages again

    # Put back nearfull default
    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_nearfull_ratio .9')
    time.sleep(10)

    # State NEAR -> NONE
    log.info('6. Verify no warning messages when nothing exceeded')

    # Check nothing logged

    # State NONE -> FULL
    log.info('7. Verify warning messages again when exceeding full_ratio')

    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .00001')
    time.sleep(10)

    # Check log

    # State FULL -> NONE
    manager.raw_cluster_cmd('tell', 'osd.0', 'injectargs', '--osd_failsafe_full_ratio .97')
    time.sleep(10)

    # Check no log

    if error == 0:
        log.info('Test Passed')
    else
        log.error('Test failures: %d', error)

   """

