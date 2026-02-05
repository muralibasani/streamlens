import { useEffect } from "react";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { insertClusterSchema, type InsertCluster } from "@/lib/api";
import { useUpdateCluster } from "@/hooks/use-kafka";
import { useToast } from "@/hooks/use-toast";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Server } from "lucide-react";

interface EditClusterDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  cluster: {
    id: number;
    name: string;
    bootstrapServers: string;
    schemaRegistryUrl?: string | null;
    connectUrl?: string | null;
    jmxHost?: string | null;
    jmxPort?: number | null;
  };
}

export function EditClusterDialog({ open, onOpenChange, cluster }: EditClusterDialogProps) {
  const { toast } = useToast();
  const updateCluster = useUpdateCluster();

  const form = useForm<InsertCluster>({
    resolver: zodResolver(insertClusterSchema),
    defaultValues: {
      name: cluster.name,
      bootstrapServers: cluster.bootstrapServers,
      schemaRegistryUrl: cluster.schemaRegistryUrl || "",
      connectUrl: cluster.connectUrl || "",
      jmxHost: cluster.jmxHost || "",
      jmxPort: cluster.jmxPort ?? undefined,
    },
  });

  // Update form values when cluster prop changes
  useEffect(() => {
    form.reset({
      name: cluster.name,
      bootstrapServers: cluster.bootstrapServers,
      schemaRegistryUrl: cluster.schemaRegistryUrl || "",
      connectUrl: cluster.connectUrl || "",
      jmxHost: cluster.jmxHost || "",
      jmxPort: cluster.jmxPort ?? undefined,
    });
  }, [cluster, form]);

  const onSubmit = (data: InsertCluster) => {
    updateCluster.mutate(
      { id: cluster.id, data },
      {
        onSuccess: () => {
          onOpenChange(false);
          toast({
            title: "Success",
            description: "Cluster updated successfully",
          });
        },
        onError: (error) => {
          toast({
            title: "Error",
            description: error.message,
            variant: "destructive",
          });
        },
      }
    );
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[425px] bg-card border-border">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Server className="w-5 h-5 text-primary" />
            Edit Kafka Cluster
          </DialogTitle>
          <DialogDescription>
            Update your cluster connection details.
          </DialogDescription>
        </DialogHeader>
        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
            <FormField
              control={form.control}
              name="name"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Cluster Name</FormLabel>
                  <FormControl>
                    <Input placeholder="Production US-East" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="bootstrapServers"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Bootstrap Servers</FormLabel>
                  <FormControl>
                    <Input placeholder="localhost:9092" {...field} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="schemaRegistryUrl"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Schema Registry URL (Optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="http://localhost:8081" value={field.value || ''} onChange={field.onChange} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="connectUrl"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Kafka Connect URL (Optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="http://localhost:8083" value={field.value || ''} onChange={field.onChange} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="jmxHost"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>JMX Host (Optional)</FormLabel>
                  <FormControl>
                    <Input placeholder="localhost" value={field.value || ''} onChange={field.onChange} />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <FormField
              control={form.control}
              name="jmxPort"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>JMX Port (Optional)</FormLabel>
                  <FormControl>
                    <Input
                      type="number"
                      placeholder="9999"
                      value={field.value ?? ""}
                      onChange={(e) => {
                    const raw = e.target.value;
                    if (raw === "") field.onChange(undefined);
                    else {
                      const n = parseInt(raw, 10);
                      field.onChange(Number.isNaN(n) ? undefined : n);
                    }
                  }}
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />
            <div className="flex justify-end gap-3 pt-4">
              <Button type="button" variant="outline" onClick={() => onOpenChange(false)}>
                Cancel
              </Button>
              <Button 
                type="submit" 
                disabled={updateCluster.isPending}
                className="bg-primary hover:bg-primary/90"
              >
                {updateCluster.isPending ? "Updating..." : "Update Cluster"}
              </Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}
