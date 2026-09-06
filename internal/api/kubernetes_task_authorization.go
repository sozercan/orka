package api

import (
	"context"
	"reflect"
	"strings"

	"github.com/gofiber/fiber/v3"
	authorizationv1 "k8s.io/api/authorization/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	corev1alpha1 "github.com/orka-agents/orka/api/v1alpha1"
)

func authorizeKubernetesTaskCreate(ctx context.Context, clientset kubernetes.Interface, userInfo *UserInfo, task *corev1alpha1.Task) error {
	if task == nil {
		return nil
	}
	if err := authorizeTaskWorkspaceClassUse(ctx, clientset, userInfo, task); err != nil {
		return err
	}
	if userInfo == nil || userInfo.AuthType != AuthTypeTokenReview {
		return nil
	}
	if kubernetesClientsetIsNil(clientset) {
		log.Info("task create authorization unavailable: missing Kubernetes clientset",
			"username", userInfo.Username,
			"namespace", task.Namespace,
			"task", task.Name,
		)
		return fiber.NewError(fiber.StatusForbidden, "not authorized to create tasks")
	}

	extra := make(map[string]authorizationv1.ExtraValue, len(userInfo.Extra))
	for key, values := range userInfo.Extra {
		extra[key] = authorizationv1.ExtraValue(values)
	}

	review, err := clientset.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User:   userInfo.Username,
			UID:    userInfo.UID,
			Groups: userInfo.Groups,
			Extra:  extra,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: task.Namespace,
				Verb:      "create",
				Group:     corev1alpha1.GroupVersion.Group,
				Resource:  "tasks",
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		log.Error(err, "task create authorization check failed",
			"username", userInfo.Username,
			"namespace", task.Namespace,
			"task", task.Name,
		)
		return fiber.NewError(fiber.StatusForbidden, "not authorized to create tasks")
	}
	if review == nil || !review.Status.Allowed || review.Status.Denied || review.Status.EvaluationError != "" {
		log.Info("task create authorization denied",
			"username", userInfo.Username,
			"namespace", task.Namespace,
			"task", task.Name,
		)
		return fiber.NewError(fiber.StatusForbidden, "not authorized to create tasks")
	}

	return nil
}

func kubernetesClientsetIsNil(clientset kubernetes.Interface) bool {
	if clientset == nil {
		return true
	}
	value := reflect.ValueOf(clientset)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func authorizeKubernetesResourceAction(
	ctx context.Context,
	clientset kubernetes.Interface,
	userInfo *UserInfo,
	namespace, verb, group, resource, name string,
) error {
	if userInfo == nil || userInfo.AuthType != AuthTypeTokenReview {
		return nil
	}
	if kubernetesClientsetIsNil(clientset) {
		return fiber.NewError(fiber.StatusForbidden, "not authorized")
	}
	extra := make(map[string]authorizationv1.ExtraValue, len(userInfo.Extra))
	for key, values := range userInfo.Extra {
		extra[key] = authorizationv1.ExtraValue(values)
	}
	resource, subresource, _ := strings.Cut(resource, "/")
	review, err := clientset.AuthorizationV1().SubjectAccessReviews().Create(ctx, &authorizationv1.SubjectAccessReview{
		Spec: authorizationv1.SubjectAccessReviewSpec{
			User: userInfo.Username, UID: userInfo.UID, Groups: userInfo.Groups, Extra: extra,
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: namespace, Verb: verb, Group: group, Resource: resource, Subresource: subresource, Name: name,
			},
		},
	}, metav1.CreateOptions{})
	if err != nil || review == nil || !review.Status.Allowed || review.Status.Denied || review.Status.EvaluationError != "" {
		return fiber.NewError(fiber.StatusForbidden, "not authorized")
	}
	return nil
}
