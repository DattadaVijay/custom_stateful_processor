import { useState, useCallback } from 'react';
import { useApi } from './use-api';
import { useToast } from './use-toast';
import {
  Comment,
  CommentCreate,
  CommentUpdate,
  CommentListResponse,
  CommentPermissions,
  RatingCreate,
  RatingAggregation,
} from '@/types/comments';

export const useComments = (entityType?: string, entityId?: string) => {
  const { get, post, put, delete: deleteApi, loading } = useApi();
  const { toast } = useToast();
  const [comments, setComments] = useState<Comment[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [visibleCount, setVisibleCount] = useState(0);
  const [ratingAggregation, setRatingAggregation] = useState<RatingAggregation | null>(null);

  const fetchComments = useCallback(async (
    entityTypeParam?: string, 
    entityIdParam?: string,
    includeDeleted = false
  ) => {
    const type = entityTypeParam || entityType;
    const id = entityIdParam || entityId;
    
    if (!type || !id) {
      console.warn('useComments: entityType and entityId are required for fetchComments');
      return;
    }

    const queryParams = includeDeleted ? '?include_deleted=true' : '';
    const response = await get<CommentListResponse>(
      `/api/entities/${type}/${id}/comments${queryParams}`
    );
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to load comments: ${response.error}`,
        variant: 'destructive',
      });
      return;
    }
    
    setComments(response.data.comments || []);
    setTotalCount(response.data.total_count || 0);
    setVisibleCount(response.data.visible_count || 0);
    
    return response.data;
  }, [get, toast, entityType, entityId]);

  const createComment = useCallback(async (
    commentData: CommentCreate,
    entityTypeParam?: string,
    entityIdParam?: string
  ) => {
    const type = entityTypeParam || entityType || commentData.entity_type;
    const id = entityIdParam || entityId || commentData.entity_id;
    
    if (!type || !id) {
      throw new Error('Entity type and ID are required for creating comments');
    }

    const response = await post<Comment>(
      `/api/entities/${type}/${id}/comments`,
      commentData
    );
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to create comment: ${response.error}`,
        variant: 'destructive',
      });
      throw new Error(response.error);
    }
    
    toast({
      title: 'Success',
      description: 'Comment created successfully',
    });
    
    // Refresh comments after creation
    await fetchComments(type, id);
    
    return response.data;
  }, [post, toast, fetchComments, entityType, entityId]);

  const updateComment = useCallback(async (
    commentId: string,
    updateData: CommentUpdate
  ) => {
    const response = await put<Comment>(`/api/comments/${commentId}`, updateData);
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to update comment: ${response.error}`,
        variant: 'destructive',
      });
      throw new Error(response.error);
    }
    
    toast({
      title: 'Success',
      description: 'Comment updated successfully',
    });
    
    // Refresh comments after update
    await fetchComments();
    
    return response.data;
  }, [put, toast, fetchComments]);

  const deleteComment = useCallback(async (
    commentId: string,
    hardDelete = false
  ) => {
    const queryParams = hardDelete ? '?hard_delete=true' : '';
    const response = await deleteApi(`/api/comments/${commentId}${queryParams}`);
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to delete comment: ${response.error}`,
        variant: 'destructive',
      });
      throw new Error(response.error);
    }
    
    toast({
      title: 'Success',
      description: 'Comment deleted successfully',
    });
    
    // Refresh comments after deletion
    await fetchComments();
    
    return true;
  }, [deleteApi, toast, fetchComments]);

  const checkCommentPermissions = useCallback(async (commentId: string) => {
    const response = await get<CommentPermissions>(`/api/comments/${commentId}/permissions`);
    
    if (response.error) {
      console.error('Failed to check comment permissions:', response.error);
      return { can_modify: false, is_admin: false };
    }
    
    return response.data;
  }, [get]);

  const getComment = useCallback(async (commentId: string) => {
    const response = await get<Comment>(`/api/comments/${commentId}`);
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to get comment: ${response.error}`,
        variant: 'destructive',
      });
      throw new Error(response.error);
    }
    
    return response.data;
  }, [get, toast]);

  // =========================================================================
  // Rating-specific methods
  // =========================================================================

  const createRating = useCallback(async (
    entityTypeParam: string,
    entityIdParam: string,
    rating: number,
    reviewText?: string,
    projectId?: string
  ) => {
    const payload: RatingCreate = {
      entity_type: entityTypeParam,
      entity_id: entityIdParam,
      rating,
      comment: reviewText,
      project_id: projectId,
    };

    const response = await post<Comment>(
      `/api/entities/${entityTypeParam}/${entityIdParam}/ratings`,
      payload
    );
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to submit rating: ${response.error}`,
        variant: 'destructive',
      });
      throw new Error(response.error);
    }
    
    toast({
      title: 'Success',
      description: 'Rating submitted successfully',
    });
    
    // Refresh rating aggregation after submission
    await fetchRatingAggregation(entityTypeParam, entityIdParam);
    
    return response.data;
  }, [post, toast]);

  const fetchRatingAggregation = useCallback(async (
    entityTypeParam?: string,
    entityIdParam?: string
  ) => {
    const type = entityTypeParam || entityType;
    const id = entityIdParam || entityId;
    
    if (!type || !id) {
      console.warn('useComments: entityType and entityId are required for fetchRatingAggregation');
      return null;
    }

    const response = await get<RatingAggregation>(
      `/api/entities/${type}/${id}/ratings`
    );
    
    if (response.error) {
      console.error('Failed to fetch ratings:', response.error);
      return null;
    }
    
    setRatingAggregation(response.data);
    return response.data;
  }, [get, entityType, entityId]);

  const fetchRatingHistory = useCallback(async (
    entityTypeParam?: string,
    entityIdParam?: string,
    userOnly = false
  ) => {
    const type = entityTypeParam || entityType;
    const id = entityIdParam || entityId;
    
    if (!type || !id) {
      console.warn('useComments: entityType and entityId are required for fetchRatingHistory');
      return null;
    }

    const queryParams = userOnly ? '?user_only=true' : '';
    const response = await get<CommentListResponse>(
      `/api/entities/${type}/${id}/ratings/history${queryParams}`
    );
    
    if (response.error) {
      toast({
        title: 'Error',
        description: `Failed to load rating history: ${response.error}`,
        variant: 'destructive',
      });
      return null;
    }
    
    return response.data;
  }, [get, toast, entityType, entityId]);

  return {
    comments,
    totalCount,
    visibleCount,
    loading,
    fetchComments,
    createComment,
    updateComment,
    deleteComment,
    checkCommentPermissions,
    getComment,
    // Rating-specific exports
    ratingAggregation,
    createRating,
    fetchRatingAggregation,
    fetchRatingHistory,
  };
};