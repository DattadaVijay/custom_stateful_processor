export interface TeamRead {
  id: string;
  name: string;
  description?: string;
  domain_id?: string;
  created_at: string;
  updated_at: string;
}

export interface ProjectRead {
  id: string;
  name: string;
  description?: string;
  owner_team_id?: string;
  status?: string;
  created_at: string;
  updated_at: string;
}

export interface UserProjectAccess {
  owned_projects: ProjectRead[];
  member_projects: ProjectRead[];
  accessible_projects: ProjectRead[];
}

export interface UserProfileData {
  email: string | null;
  username: string | null;
  user: string | null;
  ip: string | null;
  groups: string[] | null;
  role: {
    id: string;
    name: string;
    description?: string;
  } | null;
  teams: TeamRead[];
  projects: UserProjectAccess;
}
