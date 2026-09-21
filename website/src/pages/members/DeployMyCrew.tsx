/**
 * Deploy my crew — where a crew goes to run somewhere other than this machine.
 *
 * SCOPE IS CREW-WIDE, NOT PER-MEMBER. A launch ships the whole local checkout
 * to one machine and names one CloudFormation stack (`tag`), so there is no
 * per-member deployment to show. That is why the trigger sits in the page
 * header beside "add a member" rather than inside a member's own drawer: a
 * per-member placement would draw the same deployment under every row and
 * imply a scope the data does not have.
 *
 * DELIBERATELY NOT the crew-summary dashboard's mechanism. That surface renders
 * content a CREW published, so it needs a sandboxed frame and a human-authored
 * template — a crew's output cannot be given the dashboard's identity. This
 * panel shows the operator their OWN cloud state, read by the host from
 * `GET /api/cloud/launch`, so it is ordinary trusted React: no sandbox, no
 * template engine, no publish path.
 *
 * READ-ONLY by construction. Creating, cancelling and tearing down a launch
 * stay in Settings > Remote crew, which owns the whole set-up flow; this panel
 * exists so the state is reachable without hunting through Settings, and so a
 * reader can copy what they need to reach the machine.
 */
import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Check, Copy } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import { api, type LaunchJob, type LaunchJobStatus } from '../../api/client'
import { Btn } from '../../components/ui'
import {
  Dialog, DialogContent, DialogHeader, DialogBody, DialogFooter, DialogTitle,
} from '../../components/ui/dialog'

/** React Query key for the launch list. Exported so a caller can invalidate it
 *  after a Settings round trip without importing the whole panel. */
export const DEPLOY_LAUNCHES_QUERY_KEY = ['cloud', 'launches'] as const

/** The provisioner ids the gateway can report. A launch persisted before the
 *  provisioner seam existed loads as `aws_ec2`, so `provider_id` is always
 *  present and an unknown value means a lane this build does not know about. */
const PROVIDER_LABELS: Readonly<Record<string, string>> = {
  aws_ec2: 'EC2',
  aws_fargate: 'Fargate',
}

/**
 * How a provisioner id is drawn.
 *
 * Returns the id VERBATIM when it is not one this build knows, rather than a
 * generic word: a newer gateway can report a lane this frontend predates, and
 * showing its real id lets a reader search for it. A generic "Cloud" would make
 * the unknown case indistinguishable from a known one.
 */
export function deployProviderLabel(providerId: string): string {
  return PROVIDER_LABELS[providerId] ?? providerId
}

/**
 * Whether a launch has a machine a reader could reach right now.
 *
 * Only `done` qualifies. `running` is the launch still working, and a job that
 * `failed` or was `cancelled` can still carry an `instance_id` from the attempt
 * that got that far — offering it as reachable would send the reader at a
 * machine that is gone or half-built.
 */
export function deployIsReachable(job: Pick<LaunchJob, 'status' | 'instance_id'>): boolean {
  return job.status === 'done' && !!job.instance_id
}

/** Statuses that mean the launch is still moving, so the panel says to wait
 *  rather than presenting the row as a final state. */
const IN_FLIGHT: ReadonlySet<LaunchJobStatus> = new Set<LaunchJobStatus>([
  'pending', 'running', 'awaiting_signin',
])

export function deployIsInFlight(status: LaunchJobStatus): boolean {
  return IN_FLIGHT.has(status)
}

/** One copyable value. The button is the only affordance, so the value itself
 *  stays selectable text — a reader with no clipboard permission can still
 *  read it. */
function CopyRow({ label, value, testid }: { label: string; value: string; testid: string }) {
  const { t } = useTranslation()
  const [done, setDone] = useState(false)
  return (
    <div className="flex items-start gap-2" data-testid={testid}>
      <span className="w-28 shrink-0 text-[11.5px] text-muted pt-1">{label}</span>
      <code className="flex-1 min-w-0 break-all text-[12px] text-text bg-bg-hover rounded px-2 py-1">
        {value}
      </code>
      <button
        type="button"
        onClick={() => {
          // Best effort: a denied clipboard must not blank the row or throw
          // into the render tree, so the value stays readable either way.
          void navigator.clipboard?.writeText(value).then(
            () => setDone(true),
            () => undefined,
          )
        }}
        className="shrink-0 flex items-center justify-center w-7 h-7 rounded-md transition-colors bg-transparent border-none text-muted hover:text-text hover:bg-bg-hover cursor-pointer"
        aria-label={done ? t('pages.membersPage.deploy_copied') : t('pages.membersPage.deploy_copy')}
        title={done ? t('pages.membersPage.deploy_copied') : t('pages.membersPage.deploy_copy')}
        data-testid={`${testid}-copy`}
      >
        {done ? <Check size={14} /> : <Copy size={14} />}
      </button>
    </div>
  )
}

/** One launch. Everything shown comes from the job record; nothing is derived
 *  from a live AWS call, so opening this panel never costs an API request
 *  against the operator's account. */
function LaunchCard({ job }: { job: LaunchJob }) {
  const { t } = useTranslation()
  const reachable = deployIsReachable(job)
  return (
    <div
      className="rounded-md border border-border p-3 flex flex-col gap-2"
      data-testid="deploy-launch"
    >
      <div className="flex items-center gap-2 min-w-0">
        <span className="font-medium text-[13px] truncate" data-testid="deploy-launch-tag">
          {job.tag}
        </span>
        <span className="shrink-0 text-[11px] text-muted rounded px-1.5 py-0.5 bg-bg-hover" data-testid="deploy-launch-provider">
          {deployProviderLabel(job.provider_id)}
        </span>
        <span className="shrink-0 text-[11px] text-muted" data-testid="deploy-launch-status">
          {job.status}
        </span>
      </div>

      <div className="text-[11.5px] text-muted" data-testid="deploy-launch-coords">
        {/* Profile can legitimately be empty (the default chain), and a bare
            "  ·  " would read as a missing value rather than an absent one. */}
        {[job.profile, job.region].filter(Boolean).join(' \u00b7 ')}
      </div>

      {deployIsInFlight(job.status) && (
        <div className="text-[11.5px] text-warn-fg" data-testid="deploy-launch-inflight">
          {t('pages.membersPage.deploy_in_flight')}
        </div>
      )}

      {job.error && (
        <div className="text-[11.5px] text-danger break-words" data-testid="deploy-launch-error">
          {job.error}
        </div>
      )}

      {reachable && job.instance_id && (
        <CopyRow
          label={t('pages.membersPage.deploy_target')}
          value={job.instance_id}
          testid="deploy-launch-target"
        />
      )}
    </div>
  )
}

/**
 * The full-window panel.
 *
 * `open` is the host's state so the trigger and the panel do not both own it.
 * The query runs only while open (`enabled`), so a page visit that never opens
 * this panel makes no launch read at all.
 */
export default function DeployMyCrewDialog({ open, onClose }: { open: boolean; onClose: () => void }) {
  const { t } = useTranslation()
  const q = useQuery({
    queryKey: DEPLOY_LAUNCHES_QUERY_KEY,
    queryFn: () => api.cloudLaunches(),
    enabled: open,
  })
  const jobs = q.data?.jobs ?? []

  return (
    <Dialog open={open} onOpenChange={(v) => { if (!v) onClose() }}>
      <DialogContent maxWidth={720} className="max-h-[86vh]">
        <DialogHeader>
          <DialogTitle>{t('pages.membersPage.deploy_title')}</DialogTitle>
        </DialogHeader>
        <DialogBody className="flex flex-col gap-3">
          {q.isPending && (
            <div className="text-[12px] text-muted" data-testid="deploy-loading">
              {t('pages.membersPage.deploy_loading')}
            </div>
          )}

          {/* A failed read is reported as unknown, never as "nothing is
              deployed": the second would tell a reader their crew is not
              running when the truth is that we could not find out. */}
          {q.isError && (
            <div className="flex flex-col gap-2" data-testid="deploy-error">
              <span className="text-[12px] text-danger">{t('pages.membersPage.deploy_error')}</span>
              <div>
                <Btn onClick={() => void q.refetch()} data-testid="deploy-retry">
                  {t('pages.membersPage.deploy_retry')}
                </Btn>
              </div>
            </div>
          )}

          {/* The common case on a fresh install, so it reads as an instruction
              with somewhere to go, not as an error. */}
          {q.isSuccess && jobs.length === 0 && (
            <div className="text-[12px] text-muted" data-testid="deploy-empty">
              {t('pages.membersPage.deploy_empty')}
            </div>
          )}

          {jobs.map((job) => <LaunchCard key={job.id} job={job} />)}
        </DialogBody>
        <DialogFooter>
          <Btn onClick={onClose} data-testid="deploy-close">
            {t('pages.membersPage.close')}
          </Btn>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
