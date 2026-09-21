/**
 * Deploy my crew — the panel's own guards.
 *
 * The helpers are asserted DIRECTLY rather than through the rendered panel:
 * each one is a decision with a wrong answer that is invisible on screen (an
 * unreachable machine offered as reachable looks identical to a reachable one),
 * and a pure call cannot pass by finding a healthy sibling row.
 *
 * The render tests pin ONE claim: a failed read and an empty result must not
 * render the same thing. Saying "no crew is deployed" when the truth is "we
 * could not find out" tells a reader their crew is not running, which is the
 * one wrong answer this panel can give that a reader would act on.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest'
import { screen, waitFor } from '@testing-library/react'
import { renderWithProviders } from '../../test/helpers'
import {
  deployIsInFlight, deployIsReachable, deployProviderLabel,
} from './DeployMyCrew'
import type { LaunchJob, LaunchJobStatus } from '../../api/client'

const cloudLaunches = vi.fn()
vi.mock('../../api/client', () => ({
  api: { cloudLaunches: (...a: unknown[]) => cloudLaunches(...a) },
}))

/** A `done` job carrying a machine, so a test that wants an unreachable one has
 *  to say which field it changed. */
function job(over: Partial<LaunchJob> = {}): LaunchJob {
  return {
    id: 'j1',
    provider_id: 'aws_fargate',
    profile: 'dev',
    region: 'us-west-2',
    size_key: 'small',
    tag: 'crew-alpha',
    status: 'done',
    steps: [],
    instance_id: 'ecs:cluster_task_runtime',
    created_at: 0,
    updated_at: 0,
    ...over,
  }
}

describe('deployProviderLabel', () => {
  it('names the two lanes this build knows', () => {
    expect(deployProviderLabel('aws_fargate')).toBe('Fargate')
    expect(deployProviderLabel('aws_ec2')).toBe('EC2')
  })

  // The negative control, and the reason the function is not a plain lookup with
  // a generic fallback: a newer gateway can report a lane this frontend
  // predates, and a generic word would make that case indistinguishable from a
  // known one. The id has to survive so a reader can search for it.
  it('returns an id it does not know VERBATIM, never a generic word', () => {
    expect(deployProviderLabel('aws_lambda_future')).toBe('aws_lambda_future')
    expect(deployProviderLabel('')).toBe('')
  })
})

describe('deployIsReachable', () => {
  it('is true only for a finished launch that carries a machine', () => {
    expect(deployIsReachable(job())).toBe(true)
  })

  it('is false while the launch is still working', () => {
    for (const status of ['pending', 'running', 'awaiting_signin'] as LaunchJobStatus[]) {
      expect(deployIsReachable(job({ status })), status).toBe(false)
    }
  })

  // The case worth the test: a failed or cancelled attempt can still carry the
  // instance_id of the machine it got as far as creating. Offering that as
  // reachable sends the reader at something half-built or already gone, and
  // every field except `status` reads exactly like the healthy row.
  it('is false for a failed or cancelled launch even when it kept its machine', () => {
    for (const status of ['failed', 'cancelled'] as LaunchJobStatus[]) {
      const j = job({ status })
      expect(j.instance_id, 'fixture must keep the machine or this proves nothing').toBeTruthy()
      expect(deployIsReachable(j), status).toBe(false)
    }
  })

  it('is false for a finished launch with no machine recorded', () => {
    expect(deployIsReachable(job({ instance_id: undefined }))).toBe(false)
  })
})

describe('deployIsInFlight', () => {
  it('holds exactly the three moving statuses', () => {
    expect(deployIsInFlight('pending')).toBe(true)
    expect(deployIsInFlight('running')).toBe(true)
    expect(deployIsInFlight('awaiting_signin')).toBe(true)
  })

  it('is false for every settled status', () => {
    for (const status of ['done', 'failed', 'cancelled'] as LaunchJobStatus[]) {
      expect(deployIsInFlight(status), status).toBe(false)
    }
  })
})

describe('the panel tells a failed read apart from an empty one', () => {
  // The mock is module-level, so its call log accumulates across cases. Without
  // this reset the "no read while closed" assertion counts the two cases above
  // and fails for a reason that has nothing to do with the panel.
  beforeEach(() => { cloudLaunches.mockReset() })

  it('reports a read failure as unknown, and never as "nothing is deployed"', async () => {
    cloudLaunches.mockRejectedValue(new Error('boom'))
    const { default: DeployMyCrewDialog } = await import('./DeployMyCrew')
    renderWithProviders(<DeployMyCrewDialog open onClose={() => {}} />)

    await waitFor(() => expect(screen.getByTestId('deploy-error')).toBeTruthy())
    // The load-bearing half: the empty-state sentence must be ABSENT, because a
    // reader who sees it concludes their crew is not running.
    expect(screen.queryByTestId('deploy-empty')).toBeNull()
    expect(screen.getByTestId('deploy-retry')).toBeTruthy()
  })

  it('reports an empty result as empty, with no error beside it', async () => {
    cloudLaunches.mockResolvedValue({ jobs: [] })
    const { default: DeployMyCrewDialog } = await import('./DeployMyCrew')
    renderWithProviders(<DeployMyCrewDialog open onClose={() => {}} />)

    await waitFor(() => expect(screen.getByTestId('deploy-empty')).toBeTruthy())
    expect(screen.queryByTestId('deploy-error')).toBeNull()
  })

  it('makes no launch read at all while it is closed', async () => {
    cloudLaunches.mockResolvedValue({ jobs: [] })
    const { default: DeployMyCrewDialog } = await import('./DeployMyCrew')
    renderWithProviders(<DeployMyCrewDialog open={false} onClose={() => {}} />)

    // Opening the page must not cost a request against the operator's account.
    await new Promise((r) => setTimeout(r, 20))
    expect(cloudLaunches).not.toHaveBeenCalled()
  })
})
